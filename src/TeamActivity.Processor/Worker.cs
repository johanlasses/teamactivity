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
    private readonly HashSet<string> seenTelemetry = [];
    private readonly HashSet<WindowKey> publishedWindows = [];
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
        logger.LogInformation("Processor subscribed to {Topic}", wildcardTopic);

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

        var dedupeKey = WindowMath.TelemetryDedupeKey(telemetry);
        var windowKey = WindowMath.Assign(telemetry, challenge.WindowSeconds);

        lock (gate)
        {
            // New run detected — reset all state so previous run's data doesn't bleed in.
            if (currentRunId is not null && currentRunId != telemetry.RunId)
            {
                logger.LogInformation("New runId detected ({NewRunId}), clearing window state from previous run.", telemetry.RunId);
                windows.Clear();
                seenTelemetry.Clear();
                publishedWindows.Clear();
            }

            currentRunId = telemetry.RunId;

            if (!seenTelemetry.Add(dedupeKey))
            {
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
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(json)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await client.PublishAsync(message, cancellationToken);
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
}
