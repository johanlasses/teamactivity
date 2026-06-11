using System.Diagnostics;
using System.Diagnostics.Metrics;
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

    private readonly SemaphoreSlim _reconnectGate = new(1, 1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var startupDelay = TimeSpan.FromSeconds(Math.Max(0, publisherOptions.Value.StartupDelaySeconds));
        if (startupDelay > TimeSpan.Zero)
        {
            await Task.Delay(startupDelay, stoppingToken);
        }

        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();

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
                lock (_runCtsGate) { _runCts?.Cancel(); }
            }
            return Task.CompletedTask;
        };
        // ─────────────────────────────────────────────────────────────────────────

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"publisher-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter
                .WithTopic(Topics.RunTrigger)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .WithTopicFilter(filter => filter
                .WithTopic(Topics.RunAbort)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build();

        // Auto-reconnect: chaos can evict the publisher from the broker mid-run. We reconnect and
        // resubscribe so the in-progress run keeps emitting telemetry once the connection recovers.
        client.DisconnectedAsync += async _ =>
        {
            if (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            logger.LogWarning("Publisher lost MQTT connection — attempting to reconnect.");
            await ReconnectAsync(client, options, subscribeOptions, stoppingToken);
        };

        await ConnectAndSubscribeAsync(client, options, subscribeOptions, stoppingToken);
        logger.LogInformation("Publisher connected and idle — waiting for a run trigger via MQTT.");

        await foreach (var trigger in triggerChannel.Reader.ReadAllAsync(stoppingToken))
        {
            logger.LogInformation(
                "Received run trigger: RunId={RunId}, DeviceCount={DeviceCount}, IntervalMs={IntervalMs}, RunWindowSeconds={RunWindowSeconds}",
                trigger.RunId, trigger.DeviceCount, trigger.IntervalMs, trigger.RunWindowSeconds);

            var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            lock (_runCtsGate) { _runCts = runCts; }
            try
            {
                await ExecuteRun(client, trigger, challenge, runCts.Token, stoppingToken);
            }
            finally
            {
                lock (_runCtsGate) { _runCts = null; }
                runCts.Dispose();
            }

            logger.LogInformation("Run {RunId} complete — returning to idle.", trigger.RunId);
        }
    }

    private async Task ConnectAndSubscribeAsync(
        IMqttClient client,
        MqttClientOptions options,
        MqttClientSubscribeOptions subscribeOptions,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("Connecting publisher to MQTT broker {Host}:{Port}", mqttOptions.Value.Host, mqttOptions.Value.Port);
        await client.ConnectAsync(options, cancellationToken);
        await client.SubscribeAsync(subscribeOptions, cancellationToken);
    }

    private async Task ReconnectAsync(
        IMqttClient client,
        MqttClientOptions options,
        MqttClientSubscribeOptions subscribeOptions,
        CancellationToken cancellationToken)
    {
        if (!await _reconnectGate.WaitAsync(0, cancellationToken))
        {
            return;
        }

        try
        {
            while (!cancellationToken.IsCancellationRequested && !client.IsConnected)
            {
                try
                {
                    await Task.Delay(Math.Max(1, publisherOptions.Value.ReconnectDelayMs), cancellationToken);
                    await ConnectAndSubscribeAsync(client, options, subscribeOptions, cancellationToken);
                    logger.LogInformation("Publisher reconnected to MQTT broker.");
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Publisher reconnect attempt failed — retrying.");
                }
            }
        }
        finally
        {
            _reconnectGate.Release();
        }
    }

    private async Task ExecuteRun(
        IMqttClient client,
        RunTriggerMessage trigger,
        ChallengeOptions challenge,
        CancellationToken runToken,
        CancellationToken stoppingToken)
    {
        ActivityContext parentContext = default;
        bool hasParent = !string.IsNullOrEmpty(trigger.TraceParent)
            && ActivityContext.TryParse(trigger.TraceParent, null, isRemote: true, out parentContext);

        using var activity = TelemetryActivitySources.Publisher.StartActivity(
            "run.execute",
            ActivityKind.Consumer,
            hasParent ? parentContext : default);
        activity?.SetTag("run.id", trigger.RunId);

        var intervalMs = trigger.IntervalMs;
        var deviceCount = Math.Max(1, trigger.DeviceCount);
        var runWindowSeconds = Math.Max(1, trigger.RunWindowSeconds);
        var messagesPerDevice = RunMath.CalculateMessagesPerDevice(runWindowSeconds, intervalMs);
        var theoreticalTotalMessages = RunMath.CalculateTheoreticalTelemetryCount(deviceCount, runWindowSeconds, intervalMs);
        var runId = trigger.RunId;
        var runStartedAtUtc = DateTimeOffset.UtcNow;
        long publishedCount = 0;
        long sequence = 1;

        // Precompute device id strings once so the hot emission loop does no per-message allocation for them.
        var deviceIds = new string[deviceCount];
        for (var i = 0; i < deviceCount; i++)
        {
            deviceIds[i] = $"device-{i + 1:000}";
        }

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this team's Publisher has started.
        // The Scoreboard transitions from Pending → Running on receipt of this message.
        //
        // We stamp the control message's publishedAtUtc with the exact scheduling base
        // (runStartedAtUtc). The Judge derives its per-window expected message counts from this
        // timestamp, so aligning it with our actual emission schedule keeps boundary windows
        // "fully observed" instead of off-by-one.
        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherStart, stoppingToken, deviceCount, intervalMs, runWindowSeconds,
            publishedAtUtc: runStartedAtUtc);
        // ─────────────────────────────────────────────────────────────────────────

        var telemetryTopic = Topics.TelemetryRaw(runId, challenge.TeamId);

        // Emit every scheduled message for every device. We deliberately do NOT stop early when the
        // wall clock passes the nominal run end: every scheduled reading counts toward Publish
        // Attainment, and dropping the final batch was the single biggest attainment leak.
        for (var emissionIndex = 0; emissionIndex < messagesPerDevice; emissionIndex++)
        {
            var scheduledAtUtc = runStartedAtUtc.AddMilliseconds((long)emissionIndex * intervalMs);
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
                    logger.LogInformation("Run {RunId} aborted — stopping emission early at index {Index}.", runId, emissionIndex);
                    break;
                }
            }

            if (runToken.IsCancellationRequested)
                break;

            var publishedAtUtc = DateTimeOffset.UtcNow;
            // Fan out all devices for this emission concurrently so the per-device interval stays tight
            // even at high device counts.
            var publishTasks = new Task[deviceCount];
            for (var deviceIndex = 0; deviceIndex < deviceCount; deviceIndex++)
            {
                var telemetry = new TelemetryMessage(
                    Topics.SchemaVersion,
                    runId,
                    challenge.TeamId,
                    deviceIds[deviceIndex],
                    sequence++,
                    scheduledAtUtc,
                    publishedAtUtc,
                    40 + (deviceIndex + 1) + emissionIndex / 10.0);

                var telemetryJson = JsonSerializer.Serialize(telemetry, JsonContract.Options);
                publishTasks[deviceIndex] = PublishTelemetry(client, telemetryTopic, telemetryJson, challenge.TeamId, stoppingToken);
            }

            await Task.WhenAll(publishTasks);
            publishedCount += deviceCount;
        }

        logger.LogInformation(
            "Run {RunId} emission finished. Published={PublishedCount}, Theoretical={TheoreticalTotalMessages}",
            runId,
            publishedCount,
            theoreticalTotalMessages);

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this run is complete.
        // The Scoreboard transitions from Running → Idle on receipt of this message.
        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherComplete, stoppingToken,
            traceParent: activity?.Id);
        // ─────────────────────────────────────────────────────────────────────────
    }

    private async Task PublishTelemetry(
        IMqttClient client,
        string topic,
        string json,
        string teamId,
        CancellationToken cancellationToken)
    {
        try
        {
            await PublishJson(client, topic, json, cancellationToken);
            TelemetryPublished.Add(1, new KeyValuePair<string, object?>("team_id", teamId));
        }
        catch (OperationCanceledException)
        {
            // Shutting down — ignore.
        }
        catch (Exception ex)
        {
            // A transient publish failure (e.g. mid-reconnect during chaos) must not abort the run.
            logger.LogWarning(ex, "Publisher failed to send a telemetry message — continuing.");
        }
    }

    private async Task PublishControl(
        IMqttClient client,
        string runId,
        string teamId,
        string eventName,
        CancellationToken cancellationToken,
        int? deviceCount = null,
        int? messageIntervalMs = null,
        int? runWindowSeconds = null,
        string? traceParent = null,
        DateTimeOffset? publishedAtUtc = null)
    {
        var control = new ControlMessage(
            Topics.SchemaVersion,
            runId,
            teamId,
            eventName,
            publishedAtUtc ?? DateTimeOffset.UtcNow)
        {
            DeviceCount = deviceCount,
            MessageIntervalMs = messageIntervalMs,
            RunWindowSeconds = runWindowSeconds,
            TraceParent = traceParent
        };

        var topic = Topics.Control(runId, teamId, eventName);
        var json = JsonSerializer.Serialize(control, JsonContract.Options);

        // Control messages drive the Scoreboard/Judge lifecycle, so retry briefly if a publish fails
        // (e.g. a reconnect is in progress) rather than silently losing the start/complete signal.
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await PublishJson(client, topic, json, cancellationToken, MqttQualityOfServiceLevel.AtLeastOnce);
                ControlPublished.Add(1, new KeyValuePair<string, object?>("event", eventName));
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex) when (attempt < 10)
            {
                logger.LogWarning(ex, "Publisher failed to send control '{Event}' (attempt {Attempt}) — retrying.", eventName, attempt + 1);
                try
                {
                    await Task.Delay(Math.Max(1, publisherOptions.Value.ReconnectDelayMs), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private static Task PublishJson(
        IMqttClient client,
        string topic,
        string json,
        CancellationToken cancellationToken,
        MqttQualityOfServiceLevel qualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce)
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(json)
            .WithQualityOfServiceLevel(qualityOfServiceLevel)
            .Build();

        return client.PublishAsync(message, cancellationToken);
    }
}
