using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
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

    // Aggregation state — protected by _gate lock
    private readonly object _gate = new();
    private readonly Dictionary<WindowKey, WindowState> _windows = new();
    private readonly HashSet<WindowKey> _publishedWindows = new();
    private readonly HashSet<long> _dedupSet = new();
    private string? _currentRunId;
    private long _consumedCount;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var buildTime = File.GetLastWriteTimeUtc(typeof(Worker).Assembly.Location);
        logger.LogInformation("Processor started. Build={BuildTime:yyyy-MM-dd HH:mm:ss}Z, PID={PID}",
            buildTime, Environment.ProcessId);

        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        var procOpts = processorOptions.Value;
        var wildcardTopic = Topics.TelemetryRawWildcard(challenge.TeamId);

        // Resolve Mosquitto's container IP to bypass Docker NAT port-forwarding
        var mqttHost = await ResolveMosquittoContainerIp(mqtt.Host);
        var mqttPort = mqttHost != mqtt.Host ? 1883 : mqtt.Port;
        logger.LogInformation("Using MQTT broker at {Host}:{Port}", mqttHost, mqttPort);

        // Result publish channel
        var resultChannel = Channel.CreateBounded<MqttApplicationMessage>(
            new BoundedChannelOptions(procOpts.PublishChannelCapacity)
            {
                SingleWriter = true,
                SingleReader = false,
                AllowSynchronousContinuations = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        // Set up publish pool with many connections (reduces per-connection batch latency)
        await using var publishPool = new ResultPublishPool(
            procOpts.PublishConnectionCount, mqttHost, mqttPort, challenge.TeamId, logger);
        await publishPool.ConnectAllAsync(stoppingToken);
        var drainTasks = publishPool.StartDrainLoops(resultChannel.Reader, stoppingToken);

        // Single subscriber client
        var factory = new MqttClientFactory();
        using var subClient = factory.CreateMqttClient();

        subClient.ApplicationMessageReceivedAsync += args =>
        {
            HandleIncoming(args.ApplicationMessage, challenge);
            return Task.CompletedTask;
        };

        subClient.DisconnectedAsync += async args =>
        {
            if (stoppingToken.IsCancellationRequested) return;
            logger.LogWarning(
                "Subscriber disconnected: Reason={Reason}, ClientWasConnected={WasConnected}, ReasonString={ReasonString}, Exception={ExType} {ExMsg}",
                args.Reason, args.ClientWasConnected,
                args.ReasonString ?? "(null)",
                args.Exception?.GetType().Name ?? "(none)",
                args.Exception?.Message ?? "");
            await ReconnectSubscriber(subClient, mqttHost, mqttPort, wildcardTopic, challenge.TeamId, stoppingToken);
        };

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"processor-{challenge.TeamId}")
            .WithTcpServer(mqttHost, mqttPort)
            .WithCleanStart()
            .Build();

        await subClient.ConnectAsync(options, stoppingToken);
        await subClient.SubscribeAsync(
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(f => f
                    .WithTopic(wildcardTopic)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
                .Build(), stoppingToken);

        logger.LogInformation("Processor subscribed (single client) to {Topic}", wildcardTopic);

        // Flush loop
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

        // Final flush
        await FlushDueWindows(resultChannel.Writer, challenge, windowGraceMs: 0, stoppingToken);

        resultChannel.Writer.Complete();
        await Task.WhenAll(drainTasks).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        flushTimer.Dispose();
        await flushTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    /// <summary>
    /// Fast inline message handler — called sequentially from MQTTnet receive loop.
    /// Synchronized with FlushLoop via _gate lock.
    /// </summary>
    private void HandleIncoming(MqttApplicationMessage message, ChallengeOptions challenge)
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

        _consumedCount++;
        TelemetryConsumed.Add(1);

        lock (_gate)
        {
            // Handle run change
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

            // Dedup by sequence
            if (!_dedupSet.Add(telemetry.Sequence))
                return;

            var windowKey = WindowMath.Assign(telemetry, challenge.WindowSeconds);

            // Skip if already published
            if (_publishedWindows.Contains(windowKey))
                return;

            // Get or create window state and aggregate
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

    private async ValueTask FlushDueWindows(
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

        if (toPublish is null) return;

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
                return;
            }
        }
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
                    .WithCleanStart()
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

    /// <summary>
    /// Resolves the Mosquitto Docker container's IP to bypass port-forward NAT.
    /// Falls back to the configured host if Docker inspection fails.
    /// </summary>
    private async Task<string> ResolveMosquittoContainerIp(string fallbackHost)
    {
        try
        {
            var psi = new ProcessStartInfo("docker", "inspect --format={{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}} $(docker ps --filter ancestor=eclipse-mosquitto:2.0 -q)")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                FileName = "/bin/bash",
                Arguments = "-c \"docker inspect --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker ps --filter ancestor=eclipse-mosquitto:2.0 -q | head -1)\""
            };

            using var proc = Process.Start(psi);
            if (proc is null) return fallbackHost;

            var output = await proc.StandardOutput.ReadToEndAsync();
            await proc.WaitForExitAsync();

            var ip = output.Trim();
            if (proc.ExitCode == 0 && !string.IsNullOrEmpty(ip) && ip.Contains('.'))
            {
                logger.LogInformation("Resolved Mosquitto container IP: {IP} (bypassing NAT)", ip);
                return ip;
            }
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Failed to resolve Mosquitto container IP, using fallback");
        }

        return fallbackHost;
    }

    private sealed class WindowState
    {
        public int Count;
        public double Sum;
        public double Min = double.PositiveInfinity;
        public double Max = double.NegativeInfinity;
    }
}
