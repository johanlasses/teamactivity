using System.Diagnostics;
using System.Diagnostics.Metrics;
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

    private const int MaxInFlightPublishes = 256;

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

        var triggerChannel = Channel.CreateBounded<RunTriggerMessage>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });

        client.ApplicationMessageReceivedAsync += args =>
        {
            if (args.ApplicationMessage.Topic == Topics.RunTrigger)
            {
                try
                {
                    var payload = args.ApplicationMessage.Payload;
                    var reader = new Utf8JsonReader(payload);
                    var trigger = JsonSerializer.Deserialize<RunTriggerMessage>(ref reader, JsonContract.Options);
                    if (trigger is not null)
                    {
                        triggerChannel.Writer.TryWrite(trigger);
                    }
                }
                catch (JsonException ex)
                {
                    logger.LogWarning(ex, "Publisher ignored invalid run trigger payload.");
                }
            }
            else if (args.ApplicationMessage.Topic == Topics.RunAbort)
            {
                lock (_runCtsGate)
                {
                    _runCts?.Cancel();
                }
            }

            return Task.CompletedTask;
        };

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
        logger.LogInformation("Publisher connected and idle - waiting for a run trigger via MQTT.");

        await foreach (var trigger in triggerChannel.Reader.ReadAllAsync(stoppingToken))
        {
            logger.LogInformation(
            "Received run trigger: RunId={RunId}, DeviceCount={DeviceCount}, IntervalMs={IntervalMs}, RunWindowSeconds={RunWindowSeconds}",
            trigger.RunId, trigger.DeviceCount, trigger.IntervalMs, trigger.RunWindowSeconds);

            var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            lock (_runCtsGate)
            {
                _runCts = runCts;
            }

            try
            {
                await ExecuteRun(client, trigger, challenge, runCts.Token, stoppingToken);
            }
            finally
            {
                lock (_runCtsGate)
                {
                    _runCts = null;
                }

                runCts.Dispose();
            }

            logger.LogInformation("Run {RunId} complete - returning to idle.", trigger.RunId);
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

        var intervalMs = Math.Max(1, trigger.IntervalMs);
        var deviceCount = Math.Max(1, trigger.DeviceCount);
        var runWindowSeconds = Math.Max(1, trigger.RunWindowSeconds);
        var messagesPerDevice = RunMath.CalculateMessagesPerDevice(runWindowSeconds, intervalMs);
        var theoreticalTotalMessages = RunMath.CalculateTheoreticalTelemetryCount(deviceCount, runWindowSeconds, intervalMs);
        var runId = trigger.RunId;

        var runStartedAtUtc = DateTimeOffset.UtcNow;
        var runWindowMs = runWindowSeconds * 1000L;
        long publishedCount = 0;
        long sequence = 1;

        var deviceIds = new string[deviceCount];
        for (var i = 0; i < deviceCount; i++)
        {
            deviceIds[i] = $"device-{i + 1:000}";
        }

        await PublishControl(
        client,
        runId,
        challenge.TeamId,
        Topics.PublisherStart,
        stoppingToken,
        deviceCount,
        intervalMs,
        runWindowSeconds);

        var telemetryTopic = Topics.TelemetryRaw(runId, challenge.TeamId);
        var inFlight = new List<Task>(MaxInFlightPublishes);
        var scheduler = Stopwatch.StartNew();

        try
        {
            for (var emissionIndex = 0; emissionIndex < messagesPerDevice; emissionIndex++)
            {
                if (runToken.IsCancellationRequested)
                {
                    logger.LogInformation("Run {RunId} aborted - stopping emission early at index {Index}.", runId, emissionIndex);
                    break;
                }

                var targetElapsedMs = (long)emissionIndex * intervalMs;
                while (true)
                {
                    var remainingMs = targetElapsedMs - scheduler.ElapsedMilliseconds;
                    if (remainingMs <= 0)
                    {
                        break;
                    }

                    var sliceMs = (int)Math.Min(remainingMs, 25);
                    await Task.Delay(sliceMs, runToken);
                }

                if (scheduler.ElapsedMilliseconds >= runWindowMs)
                {
                    logger.LogWarning(
                    "Run {RunId} ended before all scheduled emissions could be sent. Published={PublishedCount}, Theoretical={TheoreticalTotalMessages}",
                    runId,
                    publishedCount,
                    theoreticalTotalMessages);
                    break;
                }

                var scheduledAtUtc = runStartedAtUtc.AddMilliseconds(targetElapsedMs);
                var publishedAtUtc = DateTimeOffset.UtcNow;
                var baseValue = 40 + emissionIndex * 0.1d;

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
                    baseValue + deviceIndex + 1);

                    var payload = JsonSerializer.SerializeToUtf8Bytes(telemetry, JsonContract.Options);
                    inFlight.Add(PublishTelemetry(client, telemetryTopic, payload, challenge.TeamId, runToken));
                    publishedCount++;
                    if (inFlight.Count >= MaxInFlightPublishes)
                    {
                        var completed = await Task.WhenAny(inFlight);
                        inFlight.Remove(completed);
                        await completed;
                    }
                }
            }

            if (inFlight.Count > 0)
            {
                await Task.WhenAll(inFlight);
            }
        }
        catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Run {RunId} canceled by abort signal.", runId);
        }

        await PublishControl(
        client,
        runId,
        challenge.TeamId,
        Topics.PublisherComplete,
        stoppingToken,
        traceParent: activity?.Id);
    }

private static Task PublishTelemetry(
    IMqttClient client,
    string topic,
    byte[] payload,
    string teamId,
    CancellationToken cancellationToken)
{
    TelemetryPublished.Add(1, new KeyValuePair<string, object?>("team_id", teamId));
    return PublishJson(client, topic, payload, cancellationToken);
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
        var payload = JsonSerializer.SerializeToUtf8Bytes(control, JsonContract.Options);
        var publishTask = PublishJson(client, topic, payload, cancellationToken, MqttQualityOfServiceLevel.AtLeastOnce);
        ControlPublished.Add(1, new KeyValuePair<string, object?>("event", eventName));
        return publishTask;
    }

    private static Task PublishJson(
    IMqttClient client,
    string topic,
    byte[] payload,
    CancellationToken cancellationToken,
    MqttQualityOfServiceLevel qualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce)
    {
        var message = new MqttApplicationMessageBuilder()
        .WithTopic(topic)
        .WithPayload(payload)
        .WithQualityOfServiceLevel(qualityOfServiceLevel)
        .Build();

        return client.PublishAsync(message, cancellationToken);
    }
}