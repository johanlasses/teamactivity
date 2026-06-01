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

        logger.LogInformation("Connecting publisher to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        await client.ConnectAsync(options, stoppingToken);

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter
                .WithTopic(Topics.RunTrigger)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .WithTopicFilter(filter => filter
                .WithTopic(Topics.RunAbort)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build();

        await client.SubscribeAsync(subscribeOptions, stoppingToken);
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
        var runEndsAtUtc = runStartedAtUtc.AddSeconds(runWindowSeconds);
        long publishedCount = 0;
        long sequence = 1;

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this team's Publisher has started.
        // The Scoreboard transitions from Pending → Running on receipt of this message.
        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherStart, stoppingToken, deviceCount, intervalMs, runWindowSeconds);
        // ─────────────────────────────────────────────────────────────────────────

        var telemetryTopic = Topics.TelemetryRaw(runId, challenge.TeamId);

        // ── YOUR CODE ─────────────────────────────────────────────────────────────
        // Improve this loop to maximise your score:
        //   - Make timing more precise so the interval score stays high under load
        //   - Tune the `value` formula (the Judge doesn't care what values you emit,
        //     but consistency helps when debugging aggregate correctness)
        //   - Parallelise per-device publishing if you want to push interval lower
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

            if (DateTimeOffset.UtcNow >= runEndsAtUtc)
            {
                logger.LogWarning(
                    "Run {RunId} ended before all scheduled emissions could be sent. Published={PublishedCount}, Theoretical={TheoreticalTotalMessages}",
                    runId,
                    publishedCount,
                    theoreticalTotalMessages);
                break;
            }

            for (var deviceNumber = 1; deviceNumber <= deviceCount; deviceNumber++)
            {
                var publishedAtUtc = DateTimeOffset.UtcNow;
                var telemetry = new TelemetryMessage(
                    Topics.SchemaVersion,
                    runId,
                    challenge.TeamId,
                    $"device-{deviceNumber:000}",
                    sequence++,
                    scheduledAtUtc,
                    publishedAtUtc,
                    40 + deviceNumber + emissionIndex / 10.0);

                var telemetryJson = JsonSerializer.Serialize(telemetry, JsonContract.Options);
                await PublishJson(client, telemetryTopic, telemetryJson, stoppingToken);
                publishedCount++;
                TelemetryPublished.Add(1, new KeyValuePair<string, object?>("team_id", challenge.TeamId));
            }
        }
        // ─────────────────────────────────────────────────────────────────────────

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this run is complete.
        // The Scoreboard transitions from Running → Idle on receipt of this message.
        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherComplete, stoppingToken,
            traceParent: activity?.Id);
        // ─────────────────────────────────────────────────────────────────────────
    }

    private static Task PublishControl(
        IMqttClient client,
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
        var publishTask = PublishJson(client, topic, json, cancellationToken, MqttQualityOfServiceLevel.AtLeastOnce);
        ControlPublished.Add(1, new KeyValuePair<string, object?>("event", eventName));
        return publishTask;
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
