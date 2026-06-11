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

    private CancellationTokenSource? _runCts;
    private readonly Lock _runCtsGate = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var startupDelay = TimeSpan.FromSeconds(Math.Max(0, publisherOptions.Value.StartupDelaySeconds));
        if (startupDelay > TimeSpan.Zero)
            await Task.Delay(startupDelay, stoppingToken);

        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        var pubOpts = publisherOptions.Value;
        int shardCount = Math.Max(1, pubOpts.ShardCount);

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // The Scoreboard sends a RunTriggerMessage over MQTT to kick off each run.
        // This channel receives it and routes it to ExecuteRun. Without this, the
        // Scoreboard Start Run button will appear to hang indefinitely (Pending state).
        var triggerChannel = Channel.CreateBounded<RunTriggerMessage>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });

        // Build sharded MQTT clients.  Shard 0 is the "control" shard:
        // it subscribes to RunTrigger/RunAbort and sends publisher-start / publisher-complete.
        var shards = new ShardClient[shardCount];
        for (int i = 0; i < shardCount; i++)
            shards[i] = new ShardClient(i, $"publisher-{challenge.TeamId}-{i}", mqtt, logger);

        // Wire trigger/abort handler on shard 0 only
        shards[0].Client.ApplicationMessageReceivedAsync += args =>
        {
            if (args.ApplicationMessage.Topic == Topics.RunTrigger)
            {
                var trigger = JsonSerializer.Deserialize<RunTriggerMessage>(
                    args.ApplicationMessage.Payload.ToArray(), JsonContract.Options);
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

        // Connect all shards concurrently
        await Task.WhenAll(shards.Select(s => s.ConnectAsync(stoppingToken)));

        // Subscribe RunTrigger / RunAbort on shard 0
        var subOpts = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(Topics.RunTrigger)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .WithTopicFilter(f => f.WithTopic(Topics.RunAbort)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build();
        await shards[0].Client.SubscribeAsync(subOpts, stoppingToken);

        logger.LogInformation("Publisher connected ({Shards} shards) — waiting for a run trigger via MQTT.", shardCount);

        await foreach (var trigger in triggerChannel.Reader.ReadAllAsync(stoppingToken))
        {
            logger.LogInformation(
                "Received run trigger: RunId={RunId}, DeviceCount={DeviceCount}, IntervalMs={IntervalMs}, RunWindowSeconds={RunWindowSeconds}",
                trigger.RunId, trigger.DeviceCount, trigger.IntervalMs, trigger.RunWindowSeconds);

            var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            lock (_runCtsGate) { _runCts = runCts; }
            try
            {
                await ExecuteRun(shards, trigger, challenge, runCts.Token, stoppingToken);
            }
            finally
            {
                lock (_runCtsGate) { _runCts = null; }
                runCts.Dispose();
            }

            logger.LogInformation("Run {RunId} complete — returning to idle.", trigger.RunId);
        }

        foreach (var shard in shards)
            await shard.DisposeAsync();
    }

    private async Task ExecuteRun(
        ShardClient[] shards,
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

        int intervalMs = trigger.IntervalMs;
        int deviceCount = Math.Max(1, trigger.DeviceCount);
        int runWindowSeconds = Math.Max(1, trigger.RunWindowSeconds);
        int messagesPerDevice = RunMath.CalculateMessagesPerDevice(runWindowSeconds, intervalMs);
        string runId = trigger.RunId;
        string teamId = challenge.TeamId;
        int shardCount = shards.Length;

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this team's Publisher has started.
        // The Scoreboard transitions from Pending → Running on receipt of this message.
        await PublishControl(shards[0].Client, runId, teamId, Topics.PublisherStart,
            stoppingToken, deviceCount, intervalMs, runWindowSeconds);
        // ─────────────────────────────────────────────────────────────────────────

        string telemetryTopic = Topics.TelemetryRaw(runId, teamId);

        // Pre-generate device IDs once — avoids per-tick string formatting
        string[] deviceIds = new string[deviceCount];
        for (int d = 0; d < deviceCount; d++)
            deviceIds[d] = $"device-{d + 1:000}";

        // Per-device sequence counters
        var sequences = new long[deviceCount];
        Array.Fill(sequences, 1L);

        // Parallel options: high MaxDegreeOfParallelism saturates all TCP connections
        // without allocating 50K tasks at once.
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = shardCount * 256, // e.g. 2048
            CancellationToken = runToken
        };

        // Stopwatch for sub-millisecond precise scheduling
        long runStartMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var sw = Stopwatch.StartNew();

        for (int emissionIndex = 0; emissionIndex < messagesPerDevice; emissionIndex++)
        {
            long targetElapsedMs = (long)emissionIndex * intervalMs;
            long remaining = targetElapsedMs - sw.ElapsedMilliseconds;

            if (remaining > 2)
            {
                try { await Task.Delay((int)(remaining - 1), runToken); }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogInformation("Run {RunId} aborted at emission {Index}.", runId, emissionIndex);
                    break;
                }
            }

            // Spin for final sub-ms precision
            while (sw.ElapsedMilliseconds < targetElapsedMs) { /* spin */ }

            if (runToken.IsCancellationRequested) break;

            long scheduledEventMs = runStartMs + targetElapsedMs;
            var eventTime = DateTimeOffset.FromUnixTimeMilliseconds(scheduledEventMs);
            var publishedAt = DateTimeOffset.UtcNow;

            // Publish all devices concurrently.
            // Parallel.ForAsync with high DOP fires many async publishes simultaneously,
            // saturating the TCP connections without the memory cost of 50K Task objects.
            await Parallel.ForAsync(0, deviceCount, parallelOptions, async (d, ct) =>
            {
                int shardIdx = d % shardCount;
                long seq = sequences[d]++;

                var telemetry = new TelemetryMessage(
                    Topics.SchemaVersion,
                    runId, teamId, deviceIds[d],
                    seq, eventTime, publishedAt,
                    40.0 + (d + 1));

                var payload = JsonSerializer.SerializeToUtf8Bytes(telemetry, JsonContract.Options);
                var msg = new MqttApplicationMessageBuilder()
                    .WithTopic(telemetryTopic)
                    .WithPayload(payload)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                    .Build();

                await shards[shardIdx].EnsureConnectedAsync(ct);
                await shards[shardIdx].Client.PublishAsync(msg, ct);
            });

            TelemetryPublished.Add(deviceCount, new KeyValuePair<string, object?>("team_id", teamId));
        }

        // ── BOILERPLATE: DO NOT REMOVE ────────────────────────────────────────────
        // Signals the Judge and Scoreboard that this run is complete.
        // The Scoreboard transitions from Running → Idle on receipt of this message.
        await PublishControl(shards[0].Client, runId, teamId, Topics.PublisherComplete,
            stoppingToken, traceParent: activity?.Id);
        // ─────────────────────────────────────────────────────────────────────────
    }

    // Serialise one telemetry message and publish QoS 0.
    // NOTE: Now inlined into the per-shard Task.Run loop.
    // This method is kept as a dead-code guard so it can be removed later.
    // Remove together with the semaphore-based approach.

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
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(json)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        ControlPublished.Add(1, new KeyValuePair<string, object?>("event", eventName));
        return client.PublishAsync(message, cancellationToken);
    }

    // ─── Shard client: owns one IMqttClient + reconnect logic ─────────────────

    private sealed class ShardClient(int index, string clientId, MqttOptions mqtt, ILogger logger)
        : IAsyncDisposable
    {
        private readonly SemaphoreSlim _connectLock = new(1, 1);
        private bool _disposed;

        public IMqttClient Client { get; } = new MqttClientFactory().CreateMqttClient();

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            var opts = BuildOpts(cleanStart: true);
            Client.DisconnectedAsync += HandleDisconnectedAsync;
            await Client.ConnectAsync(opts, cancellationToken);
        }

        private Task HandleDisconnectedAsync(MqttClientDisconnectedEventArgs disconnectedArgs)
        {
            if (_disposed) return Task.CompletedTask;
            logger.LogWarning("Publisher shard {Index} disconnected — reconnecting.", index);
            _ = Task.Run(() => ReconnectAsync(CancellationToken.None));
            return Task.CompletedTask;
        }

        private async Task ReconnectAsync(CancellationToken cancellationToken)
        {
            for (int attempt = 1; attempt <= 20; attempt++)
            {
                if (_disposed || Client.IsConnected) return;
                await Task.Delay(100, cancellationToken);
                try
                {
                    await Client.ConnectAsync(BuildOpts(cleanStart: false), cancellationToken);
                    logger.LogInformation("Publisher shard {Index} reconnected (attempt {A}).", index, attempt);
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Publisher shard {Index} reconnect attempt {A}/20 failed.", index, attempt);
                }
            }
            logger.LogError("Publisher shard {Index} could not reconnect after 20 attempts.", index);
        }

        public async Task EnsureConnectedAsync(CancellationToken cancellationToken)
        {
            if (Client.IsConnected) return;
            await _connectLock.WaitAsync(cancellationToken);
            try
            {
                if (Client.IsConnected) return;
                await ReconnectAsync(cancellationToken);
            }
            finally { _connectLock.Release(); }
        }

        private MqttClientOptions BuildOpts(bool cleanStart) =>
            new MqttClientOptionsBuilder()
                .WithClientId(clientId)
                .WithTcpServer(mqtt.Host, mqtt.Port)
                .WithCleanStart(cleanStart)
                .Build();

        public async ValueTask DisposeAsync()
        {
            _disposed = true;
            if (Client.IsConnected)
                await Client.DisconnectAsync();
            Client.Dispose();
            _connectLock.Dispose();
        }
    }
}

// ─── IBufferWriter<byte> wrapper over a fixed-size rented byte[] ─────────────

internal sealed class FixedArrayBufferWriter(byte[] buffer) : IBufferWriter<byte>
{
    private int _written;
    public void Advance(int count) => _written += count;
    public Memory<byte> GetMemory(int sizeHint = 0) => buffer.AsMemory(_written);
    public Span<byte> GetSpan(int sizeHint = 0) => buffer.AsSpan(_written);
}
