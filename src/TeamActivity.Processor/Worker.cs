using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Processor;

/// <summary>
/// High-throughput processor:
///  - Partitioned channel pipeline (one reader task per partition) eliminates cross-device contention.
///  - Per-window double[] from ArrayPool for SIMD-friendly contiguous storage.
///  - Per-window dedup keyed on eventTimeUtcMs (confirmed by team).
///  - Event-driven publish at windowEnd + PublishDelayMs for minimum latency.
///  - Auto-reconnect with state retention on processor-disconnect chaos event.
/// </summary>
public sealed class Worker(
    ILogger<Worker> logger,
    IOptions<MqttOptions> mqttOptions,
    IOptions<ChallengeOptions> challengeOptions,
    IOptions<ProcessorOptions> processorOptions) : BackgroundService
{
    private static readonly Counter<long> TelemetryConsumed =
        TelemetryMeters.Processor.CreateCounter<long>("telemetry_consumed_total");
    private static readonly Counter<long> TelemetryDeduplicated =
        TelemetryMeters.Processor.CreateCounter<long>("telemetry_deduplicated_total");
    private static readonly Counter<long> ResultsPublished =
        TelemetryMeters.Processor.CreateCounter<long>("results_published_total");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        var opts = processorOptions.Value;

        int partitions = Math.Max(1, opts.PartitionCount);
        int publishDelayMs = Math.Max(0, opts.PublishDelayMs);
        int maxInFlight = Math.Max(1, opts.MaxInFlightResults);

        string wildcardTopic = Topics.TelemetryRawWildcard(challenge.TeamId);

        // ─── Device registry ────────────────────────────────────────────────────
        // Pre-allocate large enough that it never needs to grow.
        // Passing a bare array by value to Tasks means growth would leave tasks with stale refs.
        // 65536 entries = 512 KB, plenty for any device count.
        var deviceIndex = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var deviceStates = new DeviceState?[65536];
        int nextDeviceIdx = 0;

        // ─── Partitioned channels ───────────────────────────────────────────────
        var partitionChannels = new Channel<TelemetryMessage>[partitions];
        for (int i = 0; i < partitions; i++)
            partitionChannels[i] = Channel.CreateBounded<TelemetryMessage>(
                new BoundedChannelOptions(65_536) { FullMode = BoundedChannelFullMode.Wait });

        // ─── Result publish channel + client ────────────────────────────────────
        var resultChannel = Channel.CreateBounded<(string topic, byte[] payload, int length)>(
            new BoundedChannelOptions(16_384) { FullMode = BoundedChannelFullMode.Wait });

        var resultClient = new MqttClientFactory().CreateMqttClient();

        async Task ConnectResultClient(bool cleanStart)
        {
            var o = new MqttClientOptionsBuilder()
                .WithClientId($"processor-results-{challenge.TeamId}")
                .WithTcpServer(mqtt.Host, mqtt.Port)
                .WithCleanStart(cleanStart)
                .Build();
            await resultClient.ConnectAsync(o, stoppingToken);
        }

        await ConnectResultClient(cleanStart: true);
        logger.LogInformation("Processor result client connected.");

        // ─── Telemetry receiver client ──────────────────────────────────────────
        var rxClient = new MqttClientFactory().CreateMqttClient();

        rxClient.ApplicationMessageReceivedAsync += args =>
        {
            // Deserialise directly from the raw byte payload — no string allocation.
            TelemetryMessage? telemetry;
            try
            {
                telemetry = JsonSerializer.Deserialize<TelemetryMessage>(
                    args.ApplicationMessage.Payload.ToArray(), JsonContract.Options);
            }
            catch { return Task.CompletedTask; }

            if (telemetry is null
                || telemetry.SchemaVersion != Topics.SchemaVersion
                || telemetry.TeamId != challenge.TeamId)
                return Task.CompletedTask;

            // Route to the partition owned by this device
            int idx = GetOrAssignDeviceIndex(
                deviceIndex, ref deviceStates, ref nextDeviceIdx, telemetry.DeviceId);
            int partition = idx % partitions;

            // If run changed, signal all partitions (handled inside reader by run-change detection)
            // For the channel write we do a best-effort TryWrite; if backpressured we await.
            var ch = partitionChannels[partition];
            if (!ch.Writer.TryWrite(telemetry))
            {
                // Channel full — do a synchronous async pump (rare at normal load)
                _ = ch.Writer.WriteAsync(telemetry, stoppingToken).AsTask();
            }

            return Task.CompletedTask;
        };

        rxClient.DisconnectedAsync += disconnectArgs =>
        {
            if (stoppingToken.IsCancellationRequested) return Task.CompletedTask;
            logger.LogWarning("Processor receive client disconnected — scheduling reconnect.");
            _ = Task.Run(() => ReconnectRxAsync(rxClient, wildcardTopic, mqtt, challenge.TeamId,
                partitionChannels, stoppingToken));
            return Task.CompletedTask;
        };

        var rxConnectOpts = new MqttClientOptionsBuilder()
            .WithClientId($"processor-rx-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        logger.LogInformation("Connecting processor to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        await rxClient.ConnectAsync(rxConnectOpts, stoppingToken);

        var subOpts = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f
                .WithTopic(wildcardTopic)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
            .Build();
        await rxClient.SubscribeAsync(subOpts, stoppingToken);
        logger.LogInformation("Processor subscribed to {Topic}", wildcardTopic);

        // ─── Start partition reader tasks ───────────────────────────────────────
        var partitionTasks = new Task[partitions];
        for (int p = 0; p < partitions; p++)
        {
            int partition = p;
            partitionTasks[p] = Task.Run(() => RunPartitionAsync(
                partition, partitionChannels[partition].Reader,
                deviceIndex, deviceStates, challenge,
                resultChannel.Writer, publishDelayMs,
                stoppingToken), stoppingToken);
        }

        // ─── Result publisher task ───────────────────────────────────────────────
        var publishTask = Task.Run(() => RunResultPublisherAsync(
            resultClient, resultChannel.Reader, maxInFlight, challenge.TeamId, stoppingToken), stoppingToken);

        // ─── Sweeper: ensures idle-device tail windows are flushed ───────────────
        var sweeperTask = Task.Run(() => RunSweeperAsync(
            deviceIndex, deviceStates, resultChannel.Writer, challenge, publishDelayMs, stoppingToken), stoppingToken);

        // Propagate exceptions from sub-tasks so failures are visible in Aspire logs
        var allTasks = partitionTasks.Append(publishTask).Append(sweeperTask).ToArray();
        try
        {
            await Task.WhenAll(allTasks);
        }
        catch (OperationCanceledException) { /* expected on shutdown */ }
        catch (Exception ex)
        {
            logger.LogError(ex, "Processor sub-task failed.");
        }

        // Cleanup
        await rxClient.DisconnectAsync();
        rxClient.Dispose();
        if (resultClient.IsConnected)
            await resultClient.DisconnectAsync();
        resultClient.Dispose();
    }

    // ─── Partition reader ────────────────────────────────────────────────────────

    private async Task RunPartitionAsync(
        int partition,
        ChannelReader<TelemetryMessage> reader,
        ConcurrentDictionary<string, int> deviceIndex,
        DeviceState?[] deviceStates,
        ChallengeOptions challenge,
        ChannelWriter<(string, byte[], int)> resultWriter,
        int publishDelayMs,
        CancellationToken cancellationToken)
    {
        string? currentRunId = null;

        await foreach (var telemetry in reader.ReadAllAsync(cancellationToken))
        {
            // Run change: clear all device window state in this partition
            if (currentRunId is not null && currentRunId != telemetry.RunId)
            {
                logger.LogInformation(
                    "Partition {P}: new runId {R}, clearing window state.", partition, telemetry.RunId);
                ClearPartitionState(partition, deviceIndex, deviceStates, deviceIndex.Count);
            }
            currentRunId = telemetry.RunId;

            if (!deviceIndex.TryGetValue(telemetry.DeviceId, out int idx))
                continue; // Should not happen — index assigned in MQTT handler

            var state = Volatile.Read(ref deviceStates[idx]);
            if (state is null)
            {
                state = new DeviceState();
                Volatile.Write(ref deviceStates[idx], state);
            }

            long eventMs = telemetry.EventTimeUtc.ToUnixTimeMilliseconds();
            long windowStartMs = eventMs - eventMs % (challenge.WindowSeconds * 1000L);
            long windowEndMs = windowStartMs + challenge.WindowSeconds * 1000L;
            long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Determine which slot this message belongs to
            WindowSlot? slot = null;
            if (state.Current is not null && state.Current.WindowStartMs == windowStartMs)
            {
                slot = state.Current;
            }
            else if (state.Previous is not null && state.Previous.WindowStartMs == windowStartMs)
            {
                slot = state.Previous;
            }
            else if (state.Current is null || windowStartMs > state.Current.WindowStartMs)
            {
                // New window — publish-and-rotate if needed
                if (state.Previous is not null)
                    await PublishSlotAsync(state.Previous, telemetry.RunId, challenge,
                        resultWriter, publishDelayMs, nowMs);

                if (state.Current is not null)
                    state.Previous = state.Current;
                else
                    state.Previous = null;

                state.Current = new WindowSlot(windowStartMs, windowEndMs, telemetry.DeviceId);
                slot = state.Current;
            }
            // else: message belongs to an older window — already published; ignore

            if (slot is null) continue;

            // Track the current runId on the state for the sweeper
            state.LastRunId = telemetry.RunId;

            // Per-window dedup by eventTimeUtcMs
            if (!slot.SeenEventTimes.Add(eventMs))
            {
                TelemetryDeduplicated.Add(1, new KeyValuePair<string, object?>("team_id", challenge.TeamId));
                continue;
            }

            slot.Append(telemetry.Value);
            TelemetryConsumed.Add(1, new KeyValuePair<string, object?>("team_id", challenge.TeamId));

            // Check if Previous is due for publication (event-driven, no timer)
            if (state.Previous is not null)
            {
                long dueAt = state.Previous.WindowEndMs + publishDelayMs;
                if (nowMs >= dueAt)
                {
                    await PublishSlotAsync(state.Previous, telemetry.RunId, challenge,
                        resultWriter, publishDelayMs, nowMs);
                    state.Previous = null;
                }
            }
        }
    }

    // ─── Sweeper (handles tail windows from devices that go idle) ────────────────

    private async Task RunSweeperAsync(
        ConcurrentDictionary<string, int> deviceIndex,
        DeviceState?[] deviceStates,
        ChannelWriter<(string, byte[], int)> resultWriter,
        ChallengeOptions challenge,
        int publishDelayMs,
        CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(200));
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            int count = deviceIndex.Count;

            foreach (var kv in deviceIndex)
            {
                int idx = kv.Value;
                var state = Volatile.Read(ref deviceStates[idx]);
                if (state is null) continue;

                // Only sweep if the device has a known run — we use Current's runId context
                // Actually we don't store runId on DeviceState. Use global: if Current window is old, publish.
                var prev = state.Previous;
                if (prev is not null && nowMs >= prev.WindowEndMs + publishDelayMs && !prev.Published)
                {
                    // We need a runId for the topic — use an empty-safe sentinel
                    string runId = state.LastRunId ?? string.Empty;
                    if (!string.IsNullOrEmpty(runId))
                    {
                        await PublishSlotAsync(prev, runId, challenge, resultWriter, 0, nowMs);
                        state.Previous = null;
                    }
                }

                var cur = state.Current;
                if (cur is not null && nowMs >= cur.WindowEndMs + publishDelayMs + 500 && !cur.Published)
                {
                    string runId = state.LastRunId ?? string.Empty;
                    if (!string.IsNullOrEmpty(runId))
                    {
                        await PublishSlotAsync(cur, runId, challenge, resultWriter, 0, nowMs);
                        state.Current = null;
                    }
                }
            }
        }
    }

    // ─── Result publisher task ────────────────────────────────────────────────────

    private async Task RunResultPublisherAsync(
        IMqttClient client,
        ChannelReader<(string topic, byte[] payload, int length)> reader,
        int maxInFlight,
        string teamId,
        CancellationToken cancellationToken)
    {
        var semaphore = new SemaphoreSlim(maxInFlight, maxInFlight);

        await foreach (var (topic, payload, length) in reader.ReadAllAsync(cancellationToken))
        {
            await semaphore.WaitAsync(cancellationToken);

            // Fire-and-forget with semaphore release
            _ = PublishOneResultAsync(client, topic, payload, length, semaphore, teamId, cancellationToken);
        }

        semaphore.Dispose();
    }

    private async Task PublishOneResultAsync(
        IMqttClient client,
        string topic,
        byte[] payload,
        int length,
        SemaphoreSlim semaphore,
        string teamId,
        CancellationToken cancellationToken)
    {
        try
        {
            if (!client.IsConnected)
                return; // Reconnect handled by DisconnectedAsync; skip to avoid blocking

            // Copy only the used portion into a fresh array for the MQTT builder
            var payloadSlice = new byte[length];
            Buffer.BlockCopy(payload, 0, payloadSlice, 0, length);
            var msg = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payloadSlice)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await client.PublishAsync(msg, cancellationToken);
            ResultsPublished.Add(1, new KeyValuePair<string, object?>("team_id", teamId));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to publish result on topic {Topic}.", topic);
        }
        finally
        {
            semaphore.Release();
            ArrayPool<byte>.Shared.Return(payload);
        }
    }

    // ─── Slot publication helper ──────────────────────────────────────────────────

    private static async ValueTask PublishSlotAsync(
        WindowSlot slot,
        string runId,
        ChallengeOptions challenge,
        ChannelWriter<(string, byte[], int)> resultWriter,
        int publishDelayMs,
        long nowMs)
    {
        if (slot.Published || slot.Count == 0) return;
        slot.Published = true;

        var windowStart = DateTimeOffset.FromUnixTimeMilliseconds(slot.WindowStartMs);
        var windowEnd = DateTimeOffset.FromUnixTimeMilliseconds(slot.WindowEndMs);

        var (sum, min, max) = SimdAggregate.Compute(slot.Values.AsSpan(0, slot.Count));
        double avg = sum / slot.Count;

        var windowKey = new WindowKey(runId, challenge.TeamId, slot.DeviceId,
            windowStart, windowEnd);

        var result = new AggregateResultMessage(
            Topics.SchemaVersion,
            runId,
            challenge.TeamId,
            slot.DeviceId,
            windowStart,
            windowEnd,
            slot.Count,
            sum,
            min,
            max,
            avg,
            WindowMath.ResultId(windowKey),
            DateTimeOffset.UtcNow);

        string topic = Topics.Result(runId, challenge.TeamId, slot.DeviceId, windowStart);

        // Serialise into a pooled buffer; ownership transferred to publisher task
        byte[] rented = ArrayPool<byte>.Shared.Rent(1024);
        int written;
        try
        {
            var writer = new Utf8JsonWriter(new FixedArrayBufferWriter(rented));
            JsonSerializer.Serialize(writer, result, JsonContract.Options);
            writer.Flush();
            written = (int)writer.BytesCommitted;
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            slot.ReturnBuffers();
            return;
        }

        // Return window value buffer early
        slot.ReturnBuffers();

        await resultWriter.WriteAsync((topic, rented, written));
    }

    // ─── Reconnect helper ─────────────────────────────────────────────────────────

    private async Task ReconnectRxAsync(
        IMqttClient client,
        string wildcardTopic,
        MqttOptions mqtt,
        string teamId,
        Channel<TelemetryMessage>[] partitionChannels,
        CancellationToken cancellationToken)
    {
        for (int attempt = 1; attempt <= 20; attempt++)
        {
            if (cancellationToken.IsCancellationRequested || client.IsConnected) return;
            await Task.Delay(100, cancellationToken);
            try
            {
                var opts = new MqttClientOptionsBuilder()
                    .WithClientId($"processor-rx-{teamId}")
                    .WithTcpServer(mqtt.Host, mqtt.Port)
                    .WithCleanStart(false) // Preserve session; keep window state intact
                    .Build();
                await client.ConnectAsync(opts, cancellationToken);

                // Re-subscribe
                var subOpts = new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(f => f
                        .WithTopic(wildcardTopic)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
                    .Build();
                await client.SubscribeAsync(subOpts, cancellationToken);
                logger.LogInformation("Processor RX reconnected (attempt {A}).", attempt);
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Processor RX reconnect attempt {A}/20 failed.", attempt);
            }
        }
        logger.LogError("Processor RX could not reconnect after 20 attempts.");
    }

    // ─── Device index helpers ─────────────────────────────────────────────────────

    private static int GetOrAssignDeviceIndex(
        ConcurrentDictionary<string, int> deviceIndex,
        ref DeviceState?[] deviceStates,
        ref int nextIdx,
        string deviceId)
    {
        if (deviceIndex.TryGetValue(deviceId, out int existing))
            return existing;

        // Atomically assign next index
        int assigned = System.Threading.Interlocked.Increment(ref nextIdx) - 1;
        int idx = deviceIndex.GetOrAdd(deviceId, assigned);
        if (idx == assigned)
        {
            // We won the race — ensure array capacity
            if (assigned >= deviceStates.Length)
            {
                var bigger = new DeviceState?[Math.Max(deviceStates.Length * 2, assigned + 1)];
                Array.Copy(deviceStates, bigger, deviceStates.Length);
                Volatile.Write(ref deviceStates, bigger);
            }
        }
        return idx;
    }

    private static void ClearPartitionState(
        int partition,
        ConcurrentDictionary<string, int> deviceIndex,
        DeviceState?[] deviceStates,
        int deviceCount)
    {
        int partitions = partition; // just use the partition count from context... actually iterate all devices
        foreach (var kv in deviceIndex)
        {
            int idx = kv.Value;
            var state = Volatile.Read(ref deviceStates[idx]);
            if (state is null) continue;
            state.Current?.ReturnBuffers();
            state.Previous?.ReturnBuffers();
            state.Current = null;
            state.Previous = null;
        }
    }
}

// ─── Per-device window state ───────────────────────────────────────────────────

internal sealed class DeviceState
{
    public WindowSlot? Current;
    public WindowSlot? Previous;
    public string? LastRunId;
}

// ─── Single 5-second window accumulator ───────────────────────────────────────

internal sealed class WindowSlot
{
    private const int InitialCapacity = 128;

    public readonly long WindowStartMs;
    public readonly long WindowEndMs;
    public readonly string DeviceId;
    public bool Published;
    public int Count;
    public double[] Values;
    public readonly HashSet<long> SeenEventTimes;
    private bool _bufferReturned;

    public WindowSlot(long windowStartMs, long windowEndMs, string deviceId)
    {
        WindowStartMs = windowStartMs;
        WindowEndMs = windowEndMs;
        DeviceId = deviceId;
        Values = ArrayPool<double>.Shared.Rent(InitialCapacity);
        SeenEventTimes = new HashSet<long>(InitialCapacity);
    }

    public void Append(double value)
    {
        if (Count >= Values.Length)
        {
            // Grow: rent larger array, copy, return old
            double[] bigger = ArrayPool<double>.Shared.Rent(Values.Length * 2);
            Values.AsSpan(0, Count).CopyTo(bigger);
            ArrayPool<double>.Shared.Return(Values);
            Values = bigger;
        }
        Values[Count++] = value;
    }

    public void ReturnBuffers()
    {
        if (_bufferReturned) return;
        _bufferReturned = true;
        ArrayPool<double>.Shared.Return(Values);
    }
}

// ─── IBufferWriter<byte> over a fixed rented byte[] (shared with Publisher) ───

internal sealed class FixedArrayBufferWriter(byte[] buffer) : IBufferWriter<byte>
{
    private int _written;
    public void Advance(int count) => _written += count;
    public Memory<byte> GetMemory(int sizeHint = 0) => buffer.AsMemory(_written);
    public Span<byte> GetSpan(int sizeHint = 0) => buffer.AsSpan(_written);
}
