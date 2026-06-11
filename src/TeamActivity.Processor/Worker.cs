using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
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
    private static readonly Counter<long> TelemetryConsumed = TelemetryMeters.Processor.CreateCounter<long>("telemetry_consumed_total");
    private static readonly Counter<long> ResultsPublished = TelemetryMeters.Processor.CreateCounter<long>("results_published_total");
    private static readonly Counter<long> DuplicatesDropped = TelemetryMeters.Processor.CreateCounter<long>("telemetry_duplicates_dropped_total");

    private readonly object gate = new();
    private readonly Dictionary<WindowKey, AggregateWindow> windows = [];
    private readonly HashSet<WindowKey> publishedWindows = [];
    // Deduplication of telemetry by deviceId|sequence, mirroring the Judge so duplicate messages
    // injected during chaos do not inflate our aggregates and cause window mismatches.
    private readonly HashSet<string> seenReadings = [];
    private string? currentRunId;

    private readonly SemaphoreSlim reconnectGate = new(1, 1);

    // Subscribe with wildcards for BOTH runId and teamId. The team id is configured in the Publisher
    // (per the setup instructions) and flows through every telemetry message; the Processor derives the
    // run/team/device identity from each message rather than from its own config. This avoids the
    // common footgun where only the Publisher's teamId is changed, leaving the Processor subscribed to a
    // topic that never matches — which manifests as every window scoring "missing" (correctness 0).
    private const string TelemetryWildcardTopic = "telemetry/v1/+/+/raw";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        var wildcardTopic = TelemetryWildcardTopic;

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();

        client.ApplicationMessageReceivedAsync += args =>
        {
            HandleTelemetry(args.ApplicationMessage.Topic, Encoding.UTF8.GetString(args.ApplicationMessage.Payload), challenge);
            return Task.CompletedTask;
        };

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"processor-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter
                .WithTopic(wildcardTopic)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
            .Build();

        // Auto-reconnect: chaos can evict the client from the broker mid-run. We keep the in-memory
        // window state (it lives in this process) and simply re-establish the connection + subscription
        // so no further windows are dropped once we recover.
        client.DisconnectedAsync += async _ =>
        {
            if (stoppingToken.IsCancellationRequested)
            {
                return;
            }

            logger.LogWarning("Processor lost MQTT connection — attempting to reconnect.");
            await ReconnectAsync(client, options, subscribeOptions, stoppingToken);
        };

        await ConnectAndSubscribeAsync(client, options, subscribeOptions, wildcardTopic, stoppingToken);

        // Poll frequently so windows are published promptly after their flush delay elapses; this keeps
        // the latency (windowEnd → result received) low while the flush delay protects correctness.
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Max(1, processorOptions.Value.PollIntervalMs)));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await PublishDueWindows(client, stoppingToken);
        }
    }

    private async Task ConnectAndSubscribeAsync(
        IMqttClient client,
        MqttClientOptions options,
        MqttClientSubscribeOptions subscribeOptions,
        string wildcardTopic,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("Connecting processor to MQTT broker {Host}:{Port}", mqttOptions.Value.Host, mqttOptions.Value.Port);
        await client.ConnectAsync(options, cancellationToken);
        await client.SubscribeAsync(subscribeOptions, cancellationToken);
        logger.LogInformation("Processor subscribed to {Topic}", wildcardTopic);
    }

    private async Task ReconnectAsync(
        IMqttClient client,
        MqttClientOptions options,
        MqttClientSubscribeOptions subscribeOptions,
        CancellationToken cancellationToken)
    {
        if (!await reconnectGate.WaitAsync(0, cancellationToken))
        {
            return;
        }

        try
        {
            var wildcardTopic = TelemetryWildcardTopic;
            while (!cancellationToken.IsCancellationRequested && !client.IsConnected)
            {
                try
                {
                    await Task.Delay(Math.Max(1, processorOptions.Value.ReconnectDelayMs), cancellationToken);
                    await ConnectAndSubscribeAsync(client, options, subscribeOptions, wildcardTopic, cancellationToken);
                    logger.LogInformation("Processor reconnected to MQTT broker.");
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Processor reconnect attempt failed — retrying.");
                }
            }
        }
        finally
        {
            reconnectGate.Release();
        }
    }

    private void HandleTelemetry(string topic, string payload, ChallengeOptions challenge)
    {
        TelemetryMessage? telemetry;
        try
        {
            telemetry = JsonSerializer.Deserialize<TelemetryMessage>(payload, JsonContract.Options);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Processor ignored invalid telemetry JSON on {Topic}", topic);
            return;
        }

        if (telemetry is null || telemetry.SchemaVersion != Topics.SchemaVersion)
        {
            logger.LogWarning("Processor ignored unsupported telemetry payload on {Topic}", topic);
            return;
        }

        // Note: we intentionally do NOT filter on a configured teamId here. The Processor handles
        // whichever team the Publisher emits (identity is taken from the message), so the team name only
        // ever needs to be set in one place — the Publisher's appsettings.
        var windowKey = WindowMath.Assign(telemetry, challenge.WindowSeconds);
        var dedupeKey = WindowMath.TelemetryDedupeKey(telemetry);

        lock (gate)
        {
            // Resets window state when a new run starts so previous run data doesn't bleed into the
            // new run's aggregates.
            if (currentRunId is not null && currentRunId != telemetry.RunId)
            {
                logger.LogInformation("New runId detected ({NewRunId}), clearing window state from previous run.", telemetry.RunId);
                windows.Clear();
                publishedWindows.Clear();
                seenReadings.Clear();
            }

            currentRunId = telemetry.RunId;

            // Deduplicate identical readings (same device + sequence). The Judge counts each reading
            // once; if we double-count a duplicate our aggregate will not match and the window scores Invalid.
            if (!seenReadings.Add(dedupeKey))
            {
                DuplicatesDropped.Add(1, new KeyValuePair<string, object?>("team_id", telemetry.TeamId));
                return;
            }

            if (!windows.TryGetValue(windowKey, out var aggregate))
            {
                aggregate = new AggregateWindow();
                windows.Add(windowKey, aggregate);
            }

            aggregate.Add(telemetry.Value);
            TelemetryConsumed.Add(1, new KeyValuePair<string, object?>("team_id", telemetry.TeamId));
        }
    }

    private async Task PublishDueWindows(IMqttClient client, CancellationToken cancellationToken)
    {
        // Without a live connection we cannot publish. Skip this tick without marking anything as
        // published so the window is retried once the connection is restored.
        if (!client.IsConnected)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var flushDelay = TimeSpan.FromMilliseconds(Math.Max(0, processorOptions.Value.PublishDelayMs));
        List<(WindowKey Key, AggregateResultMessage Result)> due = [];

        lock (gate)
        {
            foreach (var (key, aggregate) in windows)
            {
                if (publishedWindows.Contains(key))
                {
                    continue;
                }

                if (now < key.WindowEndUtc + flushDelay)
                {
                    continue;
                }

                // Snapshot the aggregate under the lock so the published values are internally consistent.
                due.Add((key, new AggregateResultMessage(
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
                    DateTimeOffset.UtcNow)));
            }
        }

        if (due.Count == 0)
        {
            return;
        }

        // Publish all due windows concurrently so a large fan-out at a window boundary doesn't add
        // serial latency to the later results.
        var tasks = new List<Task>(due.Count);
        foreach (var (key, result) in due)
        {
            tasks.Add(PublishResult(client, key, result, cancellationToken));
        }

        await Task.WhenAll(tasks);
    }

    private async Task PublishResult(IMqttClient client, WindowKey key, AggregateResultMessage result, CancellationToken cancellationToken)
    {
        var topic = Topics.Result(key.RunId, key.TeamId, key.DeviceId, key.WindowStartUtc);
        var json = JsonSerializer.Serialize(result, JsonContract.Options);
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(json)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        try
        {
            await client.PublishAsync(message, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            // Leave the window unpublished so it is retried on the next tick (e.g. after reconnect).
            logger.LogWarning(ex, "Processor failed to publish window Device={DeviceId} Start={WindowStart:HH:mm:ss} — will retry.",
                key.DeviceId, key.WindowStartUtc);
            return;
        }

        lock (gate)
        {
            publishedWindows.Add(key);
        }

        ResultsPublished.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
        logger.LogInformation(
            "Window reported: Device={DeviceId} [{WindowStart:HH:mm:ss}–{WindowEnd:HH:mm:ss}] Count={Count} Avg={Avg:F2} Min={Min:F2} Max={Max:F2}",
            key.DeviceId,
            key.WindowStartUtc,
            key.WindowEndUtc,
            result.Count,
            result.Avg,
            result.Min,
            result.Max);
    }
}
