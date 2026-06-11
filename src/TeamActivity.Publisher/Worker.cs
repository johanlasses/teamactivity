using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Publisher;

public sealed class Worker(
    ILogger<Worker> logger,
    IOptions<MqttOptions> mqttOptions,
    IOptions<ChallengeOptions> challengeOptions,
    IOptions<PublisherOptions> publisherOptions) : BackgroundService
{
    private static readonly Counter<long> TelemetryPublished = TelemetryMeters.Publisher.CreateCounter<long>("telemetry_published_total");
    private static readonly Counter<long> ControlPublished = TelemetryMeters.Publisher.CreateCounter<long>("control_published_total");

    private CancellationTokenSource? _runCts;
    private readonly Lock _runCtsGate = new();
    private readonly Lock _stateGate = new();
    private ActiveRunState? _activeRun;
    private bool _subscriptionsReady;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = publisherOptions.Value;
        var startupDelay = TimeSpan.FromSeconds(Math.Max(0, options.StartupDelaySeconds));
        if (startupDelay > TimeSpan.Zero)
        {
            await Task.Delay(startupDelay, stoppingToken);
        }

        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();
        var clientOptions = CreateClientOptions(mqtt, challenge);

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // The Scoreboard sends a RunTriggerMessage over MQTT to kick off each run.
        // This channel receives it and routes it to ExecuteRun. Without this, the
        // Scoreboard Start Run button will appear to hang indefinitely (Pending state).
        var triggerChannel = Channel.CreateBounded<RunTriggerMessage>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });

        client.ApplicationMessageReceivedAsync += args =>
        {
            if (args.ApplicationMessage.Topic == Topics.RunTrigger)
            {
                var payload = Encoding.UTF8.GetString(args.ApplicationMessage.Payload);
                var trigger = JsonSerializer.Deserialize<RunTriggerMessage>(payload, JsonContract.Options);
                if (trigger is not null)
                    triggerChannel.Writer.TryWrite(trigger);
            }
            else if (args.ApplicationMessage.Topic == Topics.RunAbort)
            {
                logger.LogInformation("Abort signal received for team {TeamId}.", challenge.TeamId);
                lock (_runCtsGate) { _runCts?.Cancel(); }
            }

            return Task.CompletedTask;
        };

        client.DisconnectedAsync += args =>
        {
            _subscriptionsReady = false;
            if (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(args.Exception, "Publisher disconnected from MQTT broker.");
            }

            return Task.CompletedTask;
        };
        // ─────────────────────────────────────────────────────────────────────────

        var (resumableRun, expiredRunNeedingCompletion) = await TryLoadCheckpointAsync(stoppingToken);
        if (resumableRun is not null)
        {
            SetActiveRun(resumableRun);
            logger.LogInformation(
                "Resuming run {RunId} from checkpoint at emission {EmissionIndex} sequence {Sequence}.",
                resumableRun.RunId,
                resumableRun.NextEmissionIndex,
                resumableRun.NextSequence);
        }

        await EnsureConnectedAndSubscribedAsync(client, clientOptions, challenge, stoppingToken);

        if (expiredRunNeedingCompletion is not null)
        {
            await PublishExpiredCompletionAsync(client, clientOptions, challenge, expiredRunNeedingCompletion, stoppingToken);
        }

        if (resumableRun is not null)
        {
            await ExecuteActiveRunAsync(client, clientOptions, challenge, resumableRun, isResume: true, traceParent: null, stoppingToken);
        }

        logger.LogInformation("Publisher connected and idle - waiting for a run trigger via MQTT.");

        var idlePollInterval = TimeSpan.FromMilliseconds(Math.Max(100, options.IdlePollIntervalMs));
        while (!stoppingToken.IsCancellationRequested)
        {
            await EnsureConnectedAndSubscribedAsync(client, clientOptions, challenge, stoppingToken);

            var waitForTriggerTask = triggerChannel.Reader.WaitToReadAsync(stoppingToken).AsTask();
            var completedTask = await Task.WhenAny(waitForTriggerTask, Task.Delay(idlePollInterval, stoppingToken));
            if (completedTask != waitForTriggerTask)
            {
                continue;
            }

            if (!await waitForTriggerTask)
            {
                break;
            }

            while (triggerChannel.Reader.TryRead(out var trigger))
            {
                logger.LogInformation(
                    "Received run trigger: RunId={RunId}, DeviceCount={DeviceCount}, IntervalMs={IntervalMs}, RunWindowSeconds={RunWindowSeconds}",
                    trigger.RunId,
                    trigger.DeviceCount,
                    trigger.IntervalMs,
                    trigger.RunWindowSeconds);

                var runState = CreateRunState(trigger, challenge.TeamId);
                SetActiveRun(runState);
                await SaveCheckpointAsync(runState, stoppingToken);
                await ExecuteActiveRunAsync(client, clientOptions, challenge, runState, isResume: false, trigger.TraceParent, stoppingToken);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        var snapshot = CloneActiveRun();
        if (snapshot is not null && !snapshot.CompleteControlPublished)
        {
            await SaveCheckpointAsync(snapshot, cancellationToken);
        }

        await base.StopAsync(cancellationToken);
    }

    private async Task ExecuteActiveRunAsync(
        IMqttClient client,
        MqttClientOptions clientOptions,
        ChallengeOptions challenge,
        ActiveRunState activeRun,
        bool isResume,
        string? traceParent,
        CancellationToken stoppingToken)
    {
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        lock (_runCtsGate)
        {
            _runCts = runCts;
        }

        try
        {
            await ExecuteRun(client, clientOptions, activeRun, traceParent, isResume, challenge, runCts.Token, stoppingToken);
        }
        finally
        {
            lock (_runCtsGate)
            {
                _runCts = null;
            }

            if (stoppingToken.IsCancellationRequested)
            {
                var snapshot = CloneActiveRun();
                if (snapshot is not null && !snapshot.CompleteControlPublished)
                {
                    await SaveCheckpointAsync(snapshot, stoppingToken);
                }
            }

            if (CloneActiveRun()?.CompleteControlPublished is true)
            {
                ClearActiveRun();
            }
        }
    }

    private async Task ExecuteRun(
        IMqttClient client,
        MqttClientOptions clientOptions,
        ActiveRunState activeRun,
        string? traceParent,
        bool isResume,
        ChallengeOptions challenge,
        CancellationToken runToken,
        CancellationToken stoppingToken)
    {
            if (isResume)
            {
                logger.LogInformation(
                    "Resuming run {RunId} with next emission {NextEmissionIndex} and sequence {NextSequence}.",
                    activeRun.RunId,
                    activeRun.NextEmissionIndex,
                    activeRun.NextSequence);
            }
            else
            {
                logger.LogInformation(
                    "Starting run {RunId} with {DeviceCount} devices, interval {IntervalMs} ms, run length {RunWindowSeconds} s.",
                    activeRun.RunId,
                    activeRun.DeviceCount,
                    activeRun.IntervalMs,
                    activeRun.RunWindowSeconds);
            }

        ActivityContext parentContext = default;
        bool hasParent = !string.IsNullOrEmpty(traceParent)
            && ActivityContext.TryParse(traceParent, null, isRemote: true, out parentContext);

        using var activity = TelemetryActivitySources.Publisher.StartActivity(
            "run.execute",
            ActivityKind.Consumer,
            hasParent ? parentContext : default);
        activity?.SetTag("run.id", activeRun.RunId);

        var messagesPerDevice = RunMath.CalculateMessagesPerDevice(activeRun.RunWindowSeconds, activeRun.IntervalMs);
        var theoreticalTotalMessages = RunMath.CalculateTheoreticalTelemetryCount(activeRun.DeviceCount, activeRun.RunWindowSeconds, activeRun.IntervalMs);
        long publishedCount = 0;
        var devices = Enumerable.Range(1, activeRun.DeviceCount)
            .Select(deviceNumber => new DeviceState($"device-{deviceNumber:000}", 40 + deviceNumber))
            .ToArray();
        var telemetryTopic = Topics.TelemetryRaw(activeRun.RunId, activeRun.TeamId);
        var checkpointEveryEmissions = Math.Max(1, publisherOptions.Value.CheckpointEveryEmissions);

        await EnsureConnectedAndSubscribedAsync(client, clientOptions, challenge, stoppingToken);

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this team's Publisher has started.
        // The Scoreboard transitions from Pending → Running on receipt of this message.
        if (!activeRun.StartControlPublished)
        {
            await PublishControlAsync(
                client,
                clientOptions,
                challenge,
                activeRun.RunId,
                activeRun.TeamId,
                Topics.PublisherStart,
                stoppingToken,
                activeRun.DeviceCount,
                activeRun.IntervalMs,
                activeRun.RunWindowSeconds);
            activeRun.StartControlPublished = true;
            SetActiveRun(activeRun);
            await SaveCheckpointAsync(activeRun, stoppingToken);
        }
        // ─────────────────────────────────────────────────────────────────────────

        // ── YOUR CODE ─────────────────────────────────────────────────────────────
        // Improve this loop to maximise your score:
        //   - Make timing more precise so the interval score stays high under load
        //   - Tune the `value` formula (the Judge doesn't care what values you emit,
        //     but consistency helps when debugging aggregate correctness)
        //   - Parallelise per-device publishing if you want to push interval lower
        for (var emissionIndex = activeRun.NextEmissionIndex; emissionIndex < messagesPerDevice; emissionIndex++)
        {
            var scheduledAtUtc = activeRun.RunStartedAtUtc.AddMilliseconds((long)emissionIndex * activeRun.IntervalMs);
            var delay = scheduledAtUtc - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero)
            {
                try
                {
                    await Task.Delay(delay, runToken);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    // Run was aborted via stop signal — exit the emission loop early.
                    logger.LogInformation("Run {RunId} aborted - stopping emission early at index {Index}.", activeRun.RunId, emissionIndex);
                    break;
                }
            }

            if (runToken.IsCancellationRequested)
                break;

            if (DateTimeOffset.UtcNow >= activeRun.RunEndsAtUtc)
            {
                logger.LogWarning(
                    "Run {RunId} ended before all scheduled emissions could be sent. Published={PublishedCount}, Theoretical={TheoreticalTotalMessages}",
                    activeRun.RunId,
                    publishedCount,
                    theoreticalTotalMessages);
                break;
            }

            foreach (var device in devices)
            {
                var publishedAtUtc = DateTimeOffset.UtcNow;
                var telemetry = new TelemetryMessage(
                    Topics.SchemaVersion,
                    activeRun.RunId,
                    activeRun.TeamId,
                    device.DeviceId,
                    activeRun.NextSequence++,
                    scheduledAtUtc,
                    publishedAtUtc,
                    device.BaseValue + emissionIndex / 10.0);

                var telemetryJson = JsonSerializer.Serialize(telemetry, JsonContract.Options);
                await PublishJsonAsync(client, clientOptions, challenge, telemetryTopic, telemetryJson, stoppingToken);
                publishedCount++;
                TelemetryPublished.Add(1, new KeyValuePair<string, object?>("team_id", activeRun.TeamId));
            }

            activeRun.NextEmissionIndex = emissionIndex + 1;
            SetActiveRun(activeRun);
            if (activeRun.NextEmissionIndex % checkpointEveryEmissions == 0)
            {
                await SaveCheckpointAsync(activeRun, stoppingToken);
            }
        }
        // ─────────────────────────────────────────────────────────────────────────

        if (stoppingToken.IsCancellationRequested)
        {
            await SaveCheckpointAsync(activeRun, stoppingToken);
            return;
        }

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this run is complete.
        // The Scoreboard transitions from Running → Idle on receipt of this message.
        if (!activeRun.CompleteControlPublished)
        {
            await PublishControlAsync(
                client,
                clientOptions,
                challenge,
                activeRun.RunId,
                activeRun.TeamId,
                Topics.PublisherComplete,
                stoppingToken,
                traceParent: activity?.Id);
            activeRun.CompleteControlPublished = true;
            SetActiveRun(activeRun);
            await DeleteCheckpointAsync(stoppingToken);
        }
        // ─────────────────────────────────────────────────────────────────────────

        logger.LogInformation(
            "Run {RunId} complete - published {PublishedCount} messages of theoretical {TheoreticalTotalMessages}.",
            activeRun.RunId,
            publishedCount,
            theoreticalTotalMessages);
    }

    private async Task PublishExpiredCompletionAsync(
        IMqttClient client,
        MqttClientOptions clientOptions,
        ChallengeOptions challenge,
        ActiveRunState expiredRun,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("Checkpointed run {RunId} has already expired; sending best-effort completion.", expiredRun.RunId);
        await PublishControlAsync(
            client,
            clientOptions,
            challenge,
            expiredRun.RunId,
            expiredRun.TeamId,
            Topics.PublisherComplete,
            cancellationToken);
        await DeleteCheckpointAsync(cancellationToken);
    }

    private async Task PublishControlAsync(
        IMqttClient client,
        MqttClientOptions clientOptions,
        ChallengeOptions challenge,
        string runId,
        string teamId,
        string eventName,
        CancellationToken cancellationToken,
        int? deviceCount = null,
        int? messageIntervalMs = null,
        int? runWindowSeconds = null,
        string? traceParent = null)
    {
        var control = new ControlMessage(
            Topics.SchemaVersion,
            runId,
            teamId,
            eventName,
            DateTimeOffset.UtcNow)
        {
            DeviceCount = deviceCount,
            MessageIntervalMs = messageIntervalMs,
            RunWindowSeconds = runWindowSeconds,
            TraceParent = traceParent
        };

        var topic = Topics.Control(runId, teamId, eventName);
        var json = JsonSerializer.Serialize(control, JsonContract.Options);
        await PublishJsonAsync(client, clientOptions, challenge, topic, json, cancellationToken, MqttQualityOfServiceLevel.AtLeastOnce);
        ControlPublished.Add(1, new KeyValuePair<string, object?>("event", eventName));
    }

    private async Task PublishJsonAsync(
        IMqttClient client,
        MqttClientOptions clientOptions,
        ChallengeOptions challenge,
        string topic,
        string json,
        CancellationToken cancellationToken,
        MqttQualityOfServiceLevel qualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce)
    {
        await EnsureConnectedAndSubscribedAsync(client, clientOptions, challenge, cancellationToken);

        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(json)
            .WithQualityOfServiceLevel(qualityOfServiceLevel)
            .Build();

        await client.PublishAsync(message, cancellationToken);
    }

    private async Task EnsureConnectedAndSubscribedAsync(
        IMqttClient client,
        MqttClientOptions clientOptions,
        ChallengeOptions challenge,
        CancellationToken cancellationToken)
    {
        if (client.IsConnected && _subscriptionsReady)
        {
            return;
        }

        var attempt = 0;
        var reconnectDelayMs = Math.Max(50, publisherOptions.Value.ReconnectInitialDelayMs);
        var reconnectMaxDelayMs = Math.Max(reconnectDelayMs, publisherOptions.Value.ReconnectMaxDelayMs);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!client.IsConnected)
                {
                    logger.LogInformation(
                        attempt == 0
                            ? "Connecting publisher to MQTT broker {Host}:{Port}"
                            : "Reconnect attempt {Attempt} to MQTT broker {Host}:{Port}",
                        mqttOptions.Value.Host,
                        mqttOptions.Value.Port,
                        attempt);
                    await client.ConnectAsync(clientOptions, cancellationToken);
                    logger.LogInformation("Publisher connected to MQTT broker.");
                }

                var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(filter => filter
                        .WithTopic(Topics.RunTrigger)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                    .WithTopicFilter(filter => filter
                        .WithTopic(Topics.RunAbort)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                    .Build();

                await client.SubscribeAsync(subscribeOptions, cancellationToken);
                _subscriptionsReady = true;
                if (attempt > 0)
                {
                    logger.LogInformation("Publisher resubscribed after reconnect.");
                }

                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                attempt++;
                _subscriptionsReady = false;
                logger.LogWarning(ex, "Publisher connect attempt {Attempt} failed; retrying.", attempt);
                await Task.Delay(TimeSpan.FromMilliseconds(reconnectDelayMs), cancellationToken);
                reconnectDelayMs = Math.Min(reconnectDelayMs * 2, reconnectMaxDelayMs);
            }
        }
    }

    private MqttClientOptions CreateClientOptions(MqttOptions mqtt, ChallengeOptions challenge)
    {
        return new MqttClientOptionsBuilder()
            .WithClientId($"publisher-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();
    }

    private static ActiveRunState CreateRunState(RunTriggerMessage trigger, string teamId)
    {
        var runStartedAtUtc = DateTimeOffset.UtcNow;
        return new ActiveRunState
        {
            RunId = trigger.RunId,
            TeamId = teamId,
            DeviceCount = Math.Max(1, trigger.DeviceCount),
            IntervalMs = Math.Max(1, trigger.IntervalMs),
            RunWindowSeconds = Math.Max(1, trigger.RunWindowSeconds),
            RunStartedAtUtc = runStartedAtUtc,
            RunEndsAtUtc = runStartedAtUtc.AddSeconds(Math.Max(1, trigger.RunWindowSeconds)),
            NextSequence = 1,
            NextEmissionIndex = 0
        };
    }

    private string GetCheckpointPath()
        => Path.Combine(Environment.CurrentDirectory, publisherOptions.Value.CheckpointFileName);

    private async Task<(ActiveRunState? ResumableRun, ActiveRunState? ExpiredRunNeedingCompletion)> TryLoadCheckpointAsync(CancellationToken cancellationToken)
    {
        var checkpointPath = GetCheckpointPath();
        if (!File.Exists(checkpointPath))
        {
            return (null, null);
        }

        try
        {
            var json = await File.ReadAllTextAsync(checkpointPath, cancellationToken);
            var checkpoint = JsonSerializer.Deserialize<ActiveRunState>(json, JsonContract.Options);
            if (checkpoint is null)
            {
                return (null, null);
            }

            logger.LogInformation("Loaded publisher checkpoint for run {RunId}.", checkpoint.RunId);
            if (!checkpoint.CompleteControlPublished && checkpoint.StartControlPublished && DateTimeOffset.UtcNow >= checkpoint.RunEndsAtUtc)
            {
                return (null, checkpoint);
            }

            if (DateTimeOffset.UtcNow < checkpoint.RunEndsAtUtc && !checkpoint.CompleteControlPublished)
            {
                return (checkpoint, null);
            }

            await DeleteCheckpointAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is IOException or JsonException)
        {
            logger.LogWarning(ex, "Failed to load publisher checkpoint; ignoring it.");
        }

        return (null, null);
    }

    private async Task SaveCheckpointAsync(ActiveRunState runState, CancellationToken cancellationToken)
    {
        var checkpointPath = GetCheckpointPath();
        var json = JsonSerializer.Serialize(runState, JsonContract.Options);
        var tempPath = $"{checkpointPath}.tmp";
        await File.WriteAllTextAsync(tempPath, json, cancellationToken);
        File.Move(tempPath, checkpointPath, overwrite: true);
        logger.LogDebug("Saved publisher checkpoint for run {RunId} at emission {EmissionIndex}.", runState.RunId, runState.NextEmissionIndex);
    }

    private Task DeleteCheckpointAsync(CancellationToken cancellationToken)
    {
        var checkpointPath = GetCheckpointPath();
        if (File.Exists(checkpointPath))
        {
            File.Delete(checkpointPath);
        }

        return Task.CompletedTask;
    }

    private void SetActiveRun(ActiveRunState runState)
    {
        lock (_stateGate)
        {
            _activeRun = runState.Clone();
        }
    }

    private ActiveRunState? CloneActiveRun()
    {
        lock (_stateGate)
        {
            return _activeRun?.Clone();
        }
    }

    private void ClearActiveRun()
    {
        lock (_stateGate)
        {
            _activeRun = null;
        }
    }

    private sealed class ActiveRunState
    {
        public string RunId { get; init; } = string.Empty;

        public string TeamId { get; init; } = string.Empty;

        public int DeviceCount { get; init; }

        public int IntervalMs { get; init; }

        public int RunWindowSeconds { get; init; }

        public DateTimeOffset RunStartedAtUtc { get; init; }

        public DateTimeOffset RunEndsAtUtc { get; init; }

        public long NextSequence { get; set; }

        public int NextEmissionIndex { get; set; }

        public bool StartControlPublished { get; set; }

        public bool CompleteControlPublished { get; set; }

        public ActiveRunState Clone()
        {
            return new ActiveRunState
            {
                RunId = RunId,
                TeamId = TeamId,
                DeviceCount = DeviceCount,
                IntervalMs = IntervalMs,
                RunWindowSeconds = RunWindowSeconds,
                RunStartedAtUtc = RunStartedAtUtc,
                RunEndsAtUtc = RunEndsAtUtc,
                NextSequence = NextSequence,
                NextEmissionIndex = NextEmissionIndex,
                StartControlPublished = StartControlPublished,
                CompleteControlPublished = CompleteControlPublished
            };
        }
    }

    private sealed record DeviceState(string DeviceId, double BaseValue);
}
