using System.Buffers;
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

    private readonly object _gate = new();
    private readonly Dictionary<WindowKey, WindowState> _windows = new();
    private readonly HashSet<WindowKey> _publishedWindows = new();
    private readonly HashSet<long> _dedupSet = new();
    private string? _currentRunId;
    private long _consumedCount;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        var procOpts = processorOptions.Value;
        var wildcardTopic = Topics.TelemetryRawWildcard(challenge.TeamId);

        var ingestChannel = Channel.CreateBounded<ReadOnlyMemory<byte>>(
            new BoundedChannelOptions(200_000)
            {
                SingleWriter = true,
                SingleReader = true,
                AllowSynchronousContinuations = true,
                FullMode = BoundedChannelFullMode.DropOldest
            });

        var resultChannel = Channel.CreateBounded<MqttApplicationMessage>(
            new BoundedChannelOptions(procOpts.PublishChannelCapacity)
            {
                SingleWriter = true,
                SingleReader = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        await using var publishPool = new ResultPublishPool(
            procOpts.PublishConnectionCount, mqtt.Host, mqtt.Port, challenge.TeamId, logger);
        await publishPool.ConnectAllAsync(stoppingToken);
        var drainTasks = publishPool.StartDrainLoops(resultChannel.Reader, stoppingToken);

        var processingTask = Task.Run(() => ProcessIngestChannel(ingestChannel.Reader, challenge, stoppingToken), stoppingToken);

        var factory = new MqttClientFactory();
        using var subClient = factory.CreateMqttClient();

        subClient.ApplicationMessageReceivedAsync += args =>
        {
            var payload = args.ApplicationMessage.Payload;
            ingestChannel.Writer.TryWrite(payload.IsSingleSegment
                ? payload.First
                : new ReadOnlyMemory<byte>(payload.ToArray()));
            return Task.CompletedTask;
        };

        subClient.DisconnectedAsync += async args =>
        {
            if (stoppingToken.IsCancellationRequested) return;
            logger.LogWarning("Subscriber disconnected: {Reason}", args.Reason);
            await ReconnectSubscriber(subClient, mqtt.Host, mqtt.Port, wildcardTopic, challenge.TeamId, stoppingToken);
        };

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"processor-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
            .WithCleanSession()
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(60))
            .Build();

        await subClient.ConnectAsync(options, stoppingToken);
        await subClient.SubscribeAsync(
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(f => f
                    .WithTopic(wildcardTopic)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
                .Build(), stoppingToken);

        logger.LogInformation("Processor subscribed to {Topic}", wildcardTopic);

        var flushTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(procOpts.FlushCheckIntervalMs));
        var flushTask = Task.Run(async () =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await flushTimer.WaitForNextTickAsync(stoppingToken);
                    await FlushDueWindows(resultChannel.Writer, challenge, procOpts.WindowGraceMs, stoppingToken);
                }
                catch (OperationCanceledException) { break; }
            }
        }, stoppingToken);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) { }

        logger.LogInformation("Processor shutting down. Consumed {Count} messages", _consumedCount);
        ingestChannel.Writer.Complete();
        await processingTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await FlushDueWindows(resultChannel.Writer, challenge, windowGraceMs: 0, stoppingToken);

        resultChannel.Writer.Complete();
        await Task.WhenAll(drainTasks).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        flushTimer.Dispose();
        await flushTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private async Task ProcessIngestChannel(ChannelReader<ReadOnlyMemory<byte>> reader, ChallengeOptions challenge, CancellationToken ct)
    {
        await foreach (var payload in reader.ReadAllAsync(ct))
        {
            HandleIncoming(payload, challenge);
        }
    }

    private void HandleIncoming(ReadOnlyMemory<byte> payload, ChallengeOptions challenge)
    {
        TelemetryMessage? telemetry;
        try
        {
            var reader = new Utf8JsonReader(payload.Span);
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

        Interlocked.Increment(ref _consumedCount);
        TelemetryConsumed.Add(1);

        lock (_gate)
        {
            if (_currentRunId is not null && _currentRunId != telemetry.RunId)
            {
                _windows.Clear();
                _publishedWindows.Clear();
                _dedupSet.Clear();
                _currentRunId = telemetry.RunId;
            }
            else if (_currentRunId is null)
            {
                _currentRunId = telemetry.RunId;
            }

            if (!_dedupSet.Add(telemetry.Sequence))
                return;

            var windowKey = WindowMath.Assign(telemetry, challenge.WindowSeconds);

            if (_publishedWindows.Contains(windowKey))
                return;

            if (!_windows.TryGetValue(windowKey, out var state))
            {
                state = new WindowState();
                _windows[windowKey] = state;
            }

            state.Count++;
            state.Sum += telemetry.Value;
            if (telemetry.Value < state.Min) state.Min = telemetry.Value;
            if (telemetry.Value > state.Max) state.Max = telemetry.Value;
        }
    }

    private async ValueTask<int> FlushDueWindows(
        ChannelWriter<MqttApplicationMessage> resultWriter,
        ChallengeOptions challenge,
        int windowGraceMs,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var graceSpan = TimeSpan.FromMilliseconds(windowGraceMs);
        List<(WindowKey key, WindowState state)>? toPublish = null;

        lock (_gate)
        {
            var keysToRemove = (List<WindowKey>?)null;

            foreach (var (key, state) in _windows)
            {
                if (_publishedWindows.Contains(key))
                {
                    keysToRemove ??= new();
                    keysToRemove.Add(key);
                    continue;
                }

                if (now < key.WindowEndUtc + graceSpan)
                    continue;

                _publishedWindows.Add(key);
                keysToRemove ??= new();
                keysToRemove.Add(key);

                toPublish ??= new();
                toPublish.Add((key, state));
            }

            if (keysToRemove is not null)
            {
                foreach (var key in keysToRemove)
                    _windows.Remove(key);
            }
        }

        if (toPublish is null) return 0;

        foreach (var (key, state) in toPublish)
        {
            var avg = state.Count == 0 ? 0 : state.Sum / state.Count;
            var result = new AggregateResultMessage(
                Topics.SchemaVersion,
                key.RunId,
                key.TeamId,
                key.DeviceId,
                key.WindowStartUtc,
                key.WindowEndUtc,
                state.Count,
                state.Sum,
                state.Min,
                state.Max,
                avg,
                WindowMath.ResultId(key),
                DateTimeOffset.UtcNow);

            var topic = Topics.Result(key.RunId, key.TeamId, key.DeviceId, key.WindowStartUtc);
            var json = JsonSerializer.SerializeToUtf8Bytes(result, JsonContract.Options);
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(json)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                .Build();

            try
            {
                await resultWriter.WriteAsync(message, ct);
            }
            catch (OperationCanceledException)
            {
                return toPublish.Count;
            }
        }
        return toPublish.Count;
    }

    private async Task ReconnectSubscriber(IMqttClient client, string host, int port, string topic, string teamId, CancellationToken ct)
    {
        var attempt = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var delay = Math.Min(50 * (1 << attempt), 3000) + Random.Shared.Next(0, 50);
                await Task.Delay(delay, ct);

                var options = new MqttClientOptionsBuilder()
                    .WithClientId($"processor-{teamId}")
                    .WithTcpServer(host, port)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
                    .WithCleanSession()
                    .WithKeepAlivePeriod(TimeSpan.FromSeconds(60))
                    .Build();

                await client.ConnectAsync(options, ct);
                await client.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(f => f
                            .WithTopic(topic)
                            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
                        .Build(), ct);

                logger.LogInformation("Subscriber reconnected after {Attempts} attempts", attempt + 1);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { return; }
            catch { attempt++; }
        }
    }

    private sealed class WindowState
    {
        public int Count;
        public double Sum;
        public double Min = double.PositiveInfinity;
        public double Max = double.NegativeInfinity;
    }
}
