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
    IOptions<ChallengeOptions> challengeOptions) : BackgroundService
{
    private static readonly Counter<long> TelemetryConsumed = TelemetryMeters.Processor.CreateCounter<long>("telemetry_consumed_total");
    private static readonly Counter<long> ResultsPublished = TelemetryMeters.Processor.CreateCounter<long>("results_published_total");

    private readonly object gate = new();
    private readonly Dictionary<WindowKey, AggregateWindow> windows = [];
    private readonly HashSet<WindowKey> publishedWindows = [];
    private readonly HashSet<(string DeviceId, long Sequence)> seenMessages = [];
    private string? currentRunId;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        // Subscribe using a wildcard for runId so we receive telemetry regardless of which UUID was assigned by the trigger.
        var wildcardTopic = Topics.TelemetryRawWildcard(challenge.TeamId);

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
            .WithCleanStart(false)
            .Build();

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter
                .WithTopic(wildcardTopic)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce))
            .Build();

        // Reconnect handler: when disconnected, attempt to reconnect with exponential back-off.
        client.DisconnectedAsync += async args =>
        {
            if (stoppingToken.IsCancellationRequested)
                return;

            logger.LogWarning(args.Exception,
                "Processor disconnected from MQTT broker (Reason: {Reason}). Attempting reconnect...",
                args.Reason);

            var delay = TimeSpan.FromSeconds(2);
            const int maxRetries = 10;

            for (var attempt = 1; attempt <= maxRetries; attempt++)
            {
                if (stoppingToken.IsCancellationRequested)
                    return;

                try
                {
                    await Task.Delay(delay, stoppingToken);
                    await client.ConnectAsync(options, stoppingToken);
                    await client.SubscribeAsync(subscribeOptions, stoppingToken);
                    logger.LogInformation("Processor reconnected to MQTT broker on attempt {Attempt}.", attempt);
                    return;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Reconnect attempt {Attempt}/{MaxRetries} failed.", attempt, maxRetries);
                    delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 30));
                }
            }

            logger.LogError("Processor failed to reconnect after {MaxRetries} attempts.", maxRetries);
        };

        // Initial connection with retry logic for startup resilience.
        logger.LogInformation("Connecting processor to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        {
            var delay = TimeSpan.FromSeconds(2);
            const int maxStartupRetries = 10;

            for (var attempt = 1; attempt <= maxStartupRetries; attempt++)
            {
                try
                {
                    await client.ConnectAsync(options, stoppingToken);
                    break;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    if (attempt == maxStartupRetries)
                        throw;

                    logger.LogWarning(ex, "Initial MQTT connect attempt {Attempt}/{MaxRetries} failed. Retrying...", attempt, maxStartupRetries);
                    await Task.Delay(delay, stoppingToken);
                    delay = TimeSpan.FromSeconds(Math.Min(delay.TotalSeconds * 2, 30));
                }
            }
        }

        await client.SubscribeAsync(subscribeOptions, stoppingToken);
        logger.LogInformation("Processor subscribed to {Topic}", wildcardTopic);

        // ── IMPROVEMENT OPPORTUNITY ───────────────────────────────────────────────
        // The timer fires every 500 ms, but windows close every 5 seconds and the
        // Judge's grace period is only 2 seconds after windowEnd. Publishing sooner
        // (e.g. immediately when windowEnd passes) improves your latency score.
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await PublishDueWindows(client, challenge, stoppingToken);
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

        if (telemetry.TeamId != challenge.TeamId)
        {
            logger.LogWarning("Processor ignored telemetry for team {TeamId}", telemetry.TeamId);
            return;
        }

        var windowKey = WindowMath.Assign(telemetry, challenge.WindowSeconds);

        lock (gate)
        {
            // ── KEEP THIS ──────────────────────────────────────────────────────────
            // Resets window state when a new run starts so previous run data doesn't
            // bleed into the new run's aggregates. If you refactor state management,
            // make sure this reset still happens when the runId changes.
            if (currentRunId is not null && currentRunId != telemetry.RunId)
            {
                logger.LogInformation("New runId detected ({NewRunId}), clearing window state from previous run.", telemetry.RunId);
                windows.Clear();
                publishedWindows.Clear();
                seenMessages.Clear();
            }
            // ───────────────────────────────────────────────────────────────────────

            currentRunId = telemetry.RunId;

            // Deduplicate: skip messages already processed (same device + sequence).
            if (!seenMessages.Add((telemetry.DeviceId, telemetry.Sequence)))
            {
                logger.LogDebug("Duplicate telemetry ignored: Device={DeviceId} Sequence={Sequence}", telemetry.DeviceId, telemetry.Sequence);
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

    private async Task PublishDueWindows(IMqttClient client, ChallengeOptions challenge, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        List<(WindowKey Key, AggregateWindow Aggregate)> due = [];

        lock (gate)
        {
            foreach (var (key, aggregate) in windows)
            {
                if (publishedWindows.Contains(key))
                {
                    continue;
                }

                if (now >= key.WindowEndUtc)
                {
                    publishedWindows.Add(key);
                    due.Add((key, aggregate));
                }
            }
        }

        foreach (var (key, aggregate) in due)
        {
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
            var json = JsonSerializer.Serialize(result, JsonContract.Options);

            await PublishWithRetry(client, topic, json, cancellationToken);
            ResultsPublished.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
            logger.LogInformation(
                "Window reported: Device={DeviceId} [{WindowStart:HH:mm:ss}–{WindowEnd:HH:mm:ss}] Count={Count} Avg={Avg:F2} Min={Min:F2} Max={Max:F2}",
                key.DeviceId,
                key.WindowStartUtc,
                key.WindowEndUtc,
                aggregate.Count,
                aggregate.Avg,
                aggregate.Min,
                aggregate.Max);
        }
    }

    private async Task PublishWithRetry(
        IMqttClient client,
        string topic,
        string json,
        CancellationToken cancellationToken)
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(json)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.ExactlyOnce)
            .Build();

        var delay = TimeSpan.FromMilliseconds(500);
        const int maxAttempts = 5;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                if (!client.IsConnected)
                {
                    logger.LogDebug("Waiting for MQTT reconnection before publish attempt {Attempt}...", attempt);
                    await WaitForConnectionAsync(client, cancellationToken);
                }

                await client.PublishAsync(message, cancellationToken);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (attempt == maxAttempts)
                {
                    logger.LogError(ex, "Failed to publish to {Topic} after {MaxAttempts} attempts.", topic, maxAttempts);
                    throw;
                }

                logger.LogWarning(ex, "Publish attempt {Attempt}/{MaxAttempts} to {Topic} failed. Retrying...", attempt, maxAttempts, topic);
                await Task.Delay(delay, cancellationToken);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 5000));
            }
        }
    }

    private static async Task WaitForConnectionAsync(IMqttClient client, CancellationToken cancellationToken)
    {
        var timeout = TimeSpan.FromSeconds(30);
        var elapsed = TimeSpan.Zero;
        var poll = TimeSpan.FromMilliseconds(250);

        while (!client.IsConnected)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (elapsed >= timeout)
                throw new InvalidOperationException("Timed out waiting for MQTT reconnection.");

            await Task.Delay(poll, cancellationToken);
            elapsed += poll;
        }
    }
}
