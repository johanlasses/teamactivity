using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Processor;

public sealed class Worker(
    ILogger<Worker> logger,
    IOptions<MqttOptions> mqttOptions,
    IOptions<ChallengeOptions> challengeOptions,
    IOptions<ProcessorOptions> processorOptions) : BackgroundService
{
    private static readonly Counter<long> TelemetryConsumed =
        TelemetryMeters.Processor.CreateCounter<long>("telemetry_consumed_total");
    private static readonly Counter<long> DedupDiscarded =
        TelemetryMeters.Processor.CreateCounter<long>("dedup_discarded_total");
    private static readonly Counter<long> LateDataDiscarded =
        TelemetryMeters.Processor.CreateCounter<long>("late_data_discarded_total");
    private static readonly Counter<long> IngestDropped =
        TelemetryMeters.Processor.CreateCounter<long>("ingest_dropped_total");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        var procOpts = processorOptions.Value;
        var partitionCount = procOpts.ResolvedPartitionCount;
        var wildcardTopic = Topics.TelemetryRawWildcard(challenge.TeamId);

        // Create partitioned ingest channels
        var partitions = new Channel<TelemetryMessage>[partitionCount];
        for (int i = 0; i < partitionCount; i++)
        {
            partitions[i] = Channel.CreateBounded<TelemetryMessage>(
                new BoundedChannelOptions(procOpts.ChannelCapacity)
                {
                    SingleWriter = true,
                    SingleReader = true,
                    AllowSynchronousContinuations = false,
                    FullMode = BoundedChannelFullMode.Wait
                });
        }

        // Create result publish channel (multiple writers, multiple readers)
        var resultChannel = Channel.CreateBounded<MqttApplicationMessage>(
            new BoundedChannelOptions(procOpts.PublishChannelCapacity)
            {
                SingleWriter = false,
                SingleReader = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        // Set up publish pool
        await using var publishPool = new ResultPublishPool(
            procOpts.PublishConnectionCount, mqtt.Host, mqtt.Port, challenge.TeamId, logger);
        await publishPool.ConnectAllAsync(stoppingToken);
        var drainTasks = publishPool.StartDrainLoops(resultChannel.Reader, stoppingToken);

        // Start partition worker tasks BEFORE subscribing (so ingest capacity is ready)
        var workerTasks = new Task[partitionCount];
        for (int i = 0; i < partitionCount; i++)
        {
            var partitionId = i;
            workerTasks[i] = Task.Run(
                () => RunPartitionWorker(partitions[partitionId].Reader, resultChannel.Writer, challenge, procOpts, partitionId, stoppingToken),
                stoppingToken);
        }

        // Set up subscribe client — register handler BEFORE connect
        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();

        client.ApplicationMessageReceivedAsync += async args =>
        {
            try
            {
                await HandleIncomingAsync(args.ApplicationMessage, challenge, partitions, partitionCount, stoppingToken);
            }
            catch (OperationCanceledException) { }
        };

        // Reconnect handler
        client.DisconnectedAsync += async args =>
        {
            if (stoppingToken.IsCancellationRequested) return;
            logger.LogWarning("Processor subscribe client disconnected: {Reason}. Reconnecting...", args.Reason);
            await ReconnectSubscribeClient(client, mqtt, wildcardTopic, stoppingToken);
        };

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"processor-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        logger.LogInformation("Connecting processor to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        await client.ConnectAsync(options, stoppingToken);

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter
                .WithTopic(wildcardTopic)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
            .Build();

        await client.SubscribeAsync(subscribeOptions, stoppingToken);
        logger.LogInformation("Processor subscribed to {Topic} with {Partitions} worker partitions", wildcardTopic, partitionCount);

        // Wait for cancellation
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) { }

        // Shutdown: complete all partition channels so workers can finish
        for (int i = 0; i < partitionCount; i++)
            partitions[i].Writer.Complete();

        await Task.WhenAll(workerTasks).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        resultChannel.Writer.Complete();
        await Task.WhenAll(drainTasks).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    /// <summary>
    /// MQTT callback — parse and route to the correct partition channel.
    /// Uses WriteAsync for backpressure (stalls dispatch if channel full, which is preferred
    /// over silently dropping messages that would corrupt aggregates).
    /// </summary>
    private async ValueTask HandleIncomingAsync(
        MqttApplicationMessage message,
        ChallengeOptions challenge,
        Channel<TelemetryMessage>[] partitions,
        int partitionCount,
        CancellationToken ct)
    {
        TelemetryMessage? telemetry;
        try
        {
            var reader = new Utf8JsonReader(message.Payload);
            telemetry = JsonSerializer.Deserialize<TelemetryMessage>(ref reader, JsonContract.Options);
        }
        catch (JsonException)
        {
            return;
        }

        if (telemetry is null || telemetry.SchemaVersion != Topics.SchemaVersion)
            return;

        if (telemetry.TeamId != challenge.TeamId)
            return;

        TelemetryConsumed.Add(1);

        // Route to partition based on deviceId hash
        var partition = (telemetry.DeviceId.GetHashCode() & 0x7FFFFFFF) % partitionCount;

        // WriteAsync provides backpressure: if channel is full, we block MQTTnet dispatch
        // which causes broker-side buffering/drops visible to both Processor and Judge equally.
        // Fallback to TryWrite only on cancellation.
        if (!partitions[partition].Writer.TryWrite(telemetry))
        {
            try
            {
                await partitions[partition].Writer.WriteAsync(telemetry, ct);
            }
            catch (OperationCanceledException)
            {
                IngestDropped.Add(1);
            }
        }
    }

    /// <summary>
    /// Partition worker: aggregates telemetry, deduplicates, and flushes closed windows.
    /// Single reader per channel — no locks needed on partition-local state.
    /// </summary>
    private async Task RunPartitionWorker(
        ChannelReader<TelemetryMessage> reader,
        ChannelWriter<MqttApplicationMessage> resultWriter,
        ChallengeOptions challenge,
        ProcessorOptions procOpts,
        int partitionId,
        CancellationToken ct)
    {
        var windows = new Dictionary<WindowKey, AggregateWindow>();
        var publishedWindows = new HashSet<WindowKey>();
        var dedupSet = new HashSet<long>();
        string? currentRunId = null;
        var flushCheckMs = procOpts.FlushCheckIntervalMs;
        var windowGraceMs = procOpts.WindowGraceMs;

        while (!ct.IsCancellationRequested)
        {
            // Process available messages with a deadline-based approach
            var deadline = Environment.TickCount64 + flushCheckMs;

            while (Environment.TickCount64 < deadline)
            {
                if (reader.TryRead(out var msg))
                {
                    ProcessMessage(msg, ref currentRunId, windows, publishedWindows, dedupSet, challenge, resultWriter, ct);
                }
                else
                {
                    var remaining = (int)(deadline - Environment.TickCount64);
                    if (remaining <= 0) break;

                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    cts.CancelAfter(remaining);
                    try
                    {
                        if (!await reader.WaitToReadAsync(cts.Token))
                            return; // channel completed
                    }
                    catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                    {
                        break; // timeout, go flush
                    }
                }
            }

            // Flush closed windows
            await FlushDueWindowsAsync(windows, publishedWindows, resultWriter, windowGraceMs, ct);
        }

        // Final flush on shutdown (no grace)
        await FlushDueWindowsAsync(windows, publishedWindows, resultWriter, windowGraceMs: 0, ct);
    }

    private void ProcessMessage(
        TelemetryMessage telemetry,
        ref string? currentRunId,
        Dictionary<WindowKey, AggregateWindow> windows,
        HashSet<WindowKey> publishedWindows,
        HashSet<long> dedupSet,
        ChallengeOptions challenge,
        ChannelWriter<MqttApplicationMessage> resultWriter,
        CancellationToken ct)
    {
        // Run change detection — flush remaining old-run windows, then reset state
        if (currentRunId is not null && currentRunId != telemetry.RunId)
        {
            logger.LogInformation("Partition detected new runId ({NewRunId}), flushing and clearing state.", telemetry.RunId);
            // Flush any remaining windows from the old run before clearing
            FlushDueWindowsAsync(windows, publishedWindows, resultWriter, windowGraceMs: 0, ct)
                .AsTask().GetAwaiter().GetResult();
            windows.Clear();
            publishedWindows.Clear();
            dedupSet.Clear();
        }
        currentRunId = telemetry.RunId;

        // Deduplication: sequence is globally unique within a run
        if (!dedupSet.Add(telemetry.Sequence))
        {
            DedupDiscarded.Add(1);
            return;
        }

        var windowKey = WindowMath.Assign(telemetry, challenge.WindowSeconds);

        // Skip messages for already-published windows (late data past our grace)
        if (publishedWindows.Contains(windowKey))
        {
            LateDataDiscarded.Add(1);
            return;
        }

        if (!windows.TryGetValue(windowKey, out var aggregate))
        {
            aggregate = new AggregateWindow();
            windows.Add(windowKey, aggregate);
        }

        aggregate.Add(telemetry.Value);
    }

    private async ValueTask FlushDueWindowsAsync(
        Dictionary<WindowKey, AggregateWindow> windows,
        HashSet<WindowKey> publishedWindows,
        ChannelWriter<MqttApplicationMessage> resultWriter,
        int windowGraceMs,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var graceSpan = TimeSpan.FromMilliseconds(windowGraceMs);

        // Collect keys to flush (avoid modifying dictionary during iteration)
        List<WindowKey>? toFlush = null;

        foreach (var (key, _) in windows)
        {
            if (publishedWindows.Contains(key))
                continue;

            if (now >= key.WindowEndUtc + graceSpan)
            {
                toFlush ??= [];
                toFlush.Add(key);
            }
        }

        if (toFlush is null) return;

        foreach (var key in toFlush)
        {
            if (!windows.TryGetValue(key, out var aggregate))
                continue;

            var result = new AggregateResultMessage(
                Topics.SchemaVersion,
                key.RunId,
                key.TeamId,
                key.DeviceId,
                key.WindowStartUtc,
                key.WindowEndUtc,
                aggregate.Count,
                aggregate.Sum,
                aggregate.Min,
                aggregate.Max,
                aggregate.Avg,
                WindowMath.ResultId(key),
                DateTimeOffset.UtcNow);

            var topic = Topics.Result(key.RunId, key.TeamId, key.DeviceId, key.WindowStartUtc);
            var json = JsonSerializer.SerializeToUtf8Bytes(result, JsonContract.Options);
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(json)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            // Only mark as published after successful enqueue
            try
            {
                await resultWriter.WriteAsync(message, ct);
                publishedWindows.Add(key);
                windows.Remove(key);
            }
            catch (OperationCanceledException)
            {
                break; // Shutting down; leave unpublished windows for possible retry
            }
        }
    }

    private async Task ReconnectSubscribeClient(IMqttClient client, MqttOptions mqtt, string wildcardTopic, CancellationToken cancellationToken)
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
                    .WithClientId($"processor-{challengeOptions.Value.TeamId}")
                    .WithTcpServer(mqtt.Host, mqtt.Port)
                    .WithCleanStart()
                    .Build();

                await client.ConnectAsync(options, cancellationToken);

                var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(filter => filter
                        .WithTopic(wildcardTopic)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
                    .Build();

                await client.SubscribeAsync(subscribeOptions, cancellationToken);
                logger.LogInformation("Subscribe client reconnected after {Attempts} attempts", attempt + 1);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                attempt++;
                logger.LogDebug(ex, "Subscribe client reconnect attempt {Attempt} failed", attempt);
            }
        }
    }
}
