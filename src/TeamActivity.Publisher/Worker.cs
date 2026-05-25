using System.Buffers;
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
    private static readonly Histogram<double> TickDrift = TelemetryMeters.Publisher.CreateHistogram<double>("publish_tick_drift_ms");

    private volatile int _lastEmissionIndex;

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
            return Task.CompletedTask;
        };
        // ─────────────────────────────────────────────────────────────────────────

        // Main client reconnect handler
        client.DisconnectedAsync += async args =>
        {
            if (stoppingToken.IsCancellationRequested) return;

            logger.LogWarning("Main client disconnected: {Reason}. Reconnecting...", args.Reason);
            await ReconnectMainClient(client, mqtt, challenge, stoppingToken);
        };

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"publisher-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        logger.LogInformation("Connecting publisher to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        await client.ConnectAsync(options, stoppingToken);

        await SubscribeRunTrigger(client, stoppingToken);
        logger.LogInformation("Publisher connected and idle — waiting for a run trigger via MQTT.");

        await foreach (var trigger in triggerChannel.Reader.ReadAllAsync(stoppingToken))
        {
            logger.LogInformation(
                "Received run trigger: RunId={RunId}, DeviceCount={DeviceCount}, IntervalMs={IntervalMs}, RunWindowSeconds={RunWindowSeconds}",
                trigger.RunId, trigger.DeviceCount, trigger.IntervalMs, trigger.RunWindowSeconds);

            await ExecuteRun(client, trigger, challenge, stoppingToken);

            logger.LogInformation("Run {RunId} complete — returning to idle.", trigger.RunId);
        }
    }

    private async Task ExecuteRun(
        IMqttClient client,
        RunTriggerMessage trigger,
        ChallengeOptions challenge,
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

        var mqtt = mqttOptions.Value;
        var pubOptions = publisherOptions.Value;
        var intervalMs = trigger.IntervalMs;
        var deviceCount = Math.Max(1, trigger.DeviceCount);
        var runWindowSeconds = Math.Max(1, trigger.RunWindowSeconds);
        var messagesPerDevice = RunMath.CalculateMessagesPerDevice(runWindowSeconds, intervalMs);
        var runId = trigger.RunId;
        var runStartedAtUtc = DateTimeOffset.UtcNow;
        var runEndsAtUtc = runStartedAtUtc.AddSeconds(runWindowSeconds);
        var shardCount = pubOptions.ShardCount;

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this team's Publisher has started.
        // The Scoreboard transitions from Pending → Running on receipt of this message.
        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherStart, stoppingToken, deviceCount, intervalMs, runWindowSeconds);
        // ─────────────────────────────────────────────────────────────────────────

        var telemetryTopic = Topics.TelemetryRaw(runId, challenge.TeamId);

        // Pre-allocate device ID strings
        var deviceIds = new string[deviceCount];
        for (int d = 0; d < deviceCount; d++)
            deviceIds[d] = $"device-{d + 1:000}";

        // Create sharded channels
        var shards = new Channel<MqttApplicationMessage>[shardCount];
        for (int i = 0; i < shardCount; i++)
            shards[i] = Channel.CreateBounded<MqttApplicationMessage>(
                new BoundedChannelOptions(pubOptions.ChannelCapacity)
                {
                    SingleWriter = false,
                    SingleReader = true,
                    AllowSynchronousContinuations = false,
                    FullMode = BoundedChannelFullMode.Wait
                });

        // Create and connect the connection pool
        await using var pool = new ConnectionPool(
            pubOptions.ConnectionCount, mqtt.Host, mqtt.Port, challenge.TeamId, logger);
        await pool.ConnectAllAsync(stoppingToken);

        // Start drain loops (one task per shard)
        var drainTasks = pool.StartDrainLoops(shards, stoppingToken);

        // Tick generator loop
        long sequence = 1;
        _lastEmissionIndex = 0;
        long publishedCount = 0;

        try
        {
            for (var emissionIndex = 0; emissionIndex < messagesPerDevice; emissionIndex++)
            {
                if (stoppingToken.IsCancellationRequested) break;

                var scheduledUtc = runStartedAtUtc.AddMilliseconds((long)emissionIndex * intervalMs);
                var now = DateTimeOffset.UtcNow;

                if (scheduledUtc > now)
                {
                    await Task.Delay(scheduledUtc - now, stoppingToken);
                }

                if (DateTimeOffset.UtcNow >= runEndsAtUtc)
                {
                    logger.LogWarning(
                        "Run {RunId} ended before all emissions sent. Published={Published}, Expected={Expected}",
                        runId, publishedCount, (long)messagesPerDevice * deviceCount);
                    break;
                }

                var publishedAtUtc = DateTimeOffset.UtcNow;
                var drift = (publishedAtUtc - scheduledUtc).TotalMilliseconds;
                TickDrift.Record(drift);

                // Generate messages for all devices this tick
                for (int d = 0; d < deviceCount; d++)
                {
                    var deviceId = deviceIds[d];
                    var seq = sequence++;
                    var value = 40.0 + (d + 1) + emissionIndex / 10.0;

                    var payload = SerializeTelemetry(
                        telemetryTopic, runId, challenge.TeamId, deviceId, seq,
                        scheduledUtc, publishedAtUtc, value);

                    var shardIdx = d & (shardCount - 1);
                    await shards[shardIdx].Writer.WriteAsync(payload, stoppingToken);
                    publishedCount++;
                }

                Interlocked.Exchange(ref _lastEmissionIndex, emissionIndex);
                TelemetryPublished.Add(deviceCount, new KeyValuePair<string, object?>("team_id", challenge.TeamId));
            }
        }
        finally
        {
            // Complete all shard writers to signal drain loops to finish
            for (int i = 0; i < shardCount; i++)
                shards[i].Writer.Complete();

            // Wait for all drain tasks to finish (messages flushed to broker)
            try
            {
                await Task.WhenAll(drainTasks).WaitAsync(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (TimeoutException)
            {
                logger.LogWarning("Drain tasks did not complete within timeout for run {RunId}", runId);
            }
            catch (OperationCanceledException) { }
        }

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this run is complete.
        // The Scoreboard transitions from Running → Idle on receipt of this message.
        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherComplete, stoppingToken,
            traceParent: activity?.Id);
        // ─────────────────────────────────────────────────────────────────────────
    }

    private static MqttApplicationMessage SerializeTelemetry(
        string topic, string runId, string teamId, string deviceId,
        long sequence, DateTimeOffset eventTimeUtc, DateTimeOffset publishedAtUtc, double value)
    {
        var telemetry = new TelemetryMessage(
            Topics.SchemaVersion, runId, teamId, deviceId,
            sequence, eventTimeUtc, publishedAtUtc, value);

        var payload = JsonSerializer.SerializeToUtf8Bytes(telemetry, JsonContract.Options);

        return new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
            .Build();
    }

    private async Task ReconnectMainClient(IMqttClient client, MqttOptions mqtt, ChallengeOptions challenge, CancellationToken cancellationToken)
    {
        var baseDelayMs = 100;
        var maxDelayMs = 5000;
        var attempt = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var jitter = Random.Shared.Next(0, 100);
                var delayMs = Math.Min(baseDelayMs * (1 << attempt), maxDelayMs) + jitter;
                await Task.Delay(delayMs, cancellationToken);

                var options = new MqttClientOptionsBuilder()
                    .WithClientId($"publisher-{challenge.TeamId}")
                    .WithTcpServer(mqtt.Host, mqtt.Port)
                    .WithCleanStart()
                    .Build();

                await client.ConnectAsync(options, cancellationToken);
                await SubscribeRunTrigger(client, cancellationToken);
                logger.LogInformation("Main client reconnected after {Attempts} attempts", attempt + 1);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                attempt++;
                logger.LogDebug(ex, "Main client reconnect attempt {Attempt} failed", attempt);
            }
        }
    }

    private static Task SubscribeRunTrigger(IMqttClient client, CancellationToken cancellationToken)
    {
        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter
                .WithTopic(Topics.RunTrigger)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build();

        return client.SubscribeAsync(subscribeOptions, cancellationToken);
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
