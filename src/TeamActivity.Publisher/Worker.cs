using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Publisher;

public sealed class Worker(
    ILogger<Worker> logger,
    IOptions<MqttOptions> mqttOptions,
    IOptions<ChallengeOptions> challengeOptions,
    IOptions<PublisherOptions> publisherOptions,
    JudgePollingClient judgeClient) : BackgroundService
{
    private static readonly Counter<long> TelemetryPublished = TelemetryMeters.Publisher.CreateCounter<long>("telemetry_published_total");
    private static readonly Counter<long> ControlPublished = TelemetryMeters.Publisher.CreateCounter<long>("control_published_total");

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

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"publisher-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        logger.LogInformation("Connecting publisher to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        await client.ConnectAsync(options, stoppingToken);

        logger.LogInformation("Publisher connected and idle — waiting for a run trigger via the scoreboard or API.");

        while (!stoppingToken.IsCancellationRequested)
        {
            var config = await judgeClient.GetPendingRun(stoppingToken);
            if (config is null)
            {
                await Task.Delay(500, stoppingToken);
                continue;
            }

            logger.LogInformation(
                "Received run trigger: RunId={RunId}, DeviceCount={DeviceCount}, IntervalMs={IntervalMs}, RunWindowSeconds={RunWindowSeconds}",
                config.RunId, config.DeviceCount, config.IntervalMs, config.RunWindowSeconds);

            await judgeClient.AcknowledgeRun(config.RunId, stoppingToken);
            await ExecuteRun(client, config, challenge, stoppingToken);

            logger.LogInformation("Run {RunId} complete — returning to idle.", config.RunId);
        }
    }

    private async Task ExecuteRun(
        IMqttClient client,
        RunTriggerConfig config,
        ChallengeOptions challenge,
        CancellationToken stoppingToken)
    {
        var intervalMs = config.IntervalMs;
        var deviceCount = Math.Max(1, config.DeviceCount);
        var runWindowMs = Math.Max(1, config.RunWindowSeconds) * 1000;
        var messageCount = Math.Max(1, runWindowMs / intervalMs);
        var interval = TimeSpan.FromMilliseconds(intervalMs);
        var runId = config.RunId;

        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherStart, stoppingToken, deviceCount, intervalMs);

        var telemetryTopic = Topics.TelemetryRaw(runId, challenge.TeamId);

        for (var sequence = 1; sequence <= messageCount; sequence++)
        {
            // Check every 25 messages whether the run was stopped externally.
            if (sequence % 25 == 0 && !await judgeClient.IsRunActive(runId, stoppingToken))
            {
                logger.LogWarning("Run {RunId} was stopped externally — aborting after {Sequence} messages.", runId, sequence);
                break;
            }

            var deviceNumber = (sequence - 1) % deviceCount + 1;
            var now = DateTimeOffset.UtcNow;
            var telemetry = new TelemetryMessage(
                Topics.SchemaVersion,
                runId,
                challenge.TeamId,
                $"device-{deviceNumber:000}",
                sequence,
                now,
                now,
                40 + deviceNumber + sequence / 10.0);

            var telemetryJson = JsonSerializer.Serialize(telemetry, JsonContract.Options);
            await PublishJson(client, telemetryTopic, telemetryJson, stoppingToken);
            TelemetryPublished.Add(1, new KeyValuePair<string, object?>("team_id", challenge.TeamId));
            logger.LogInformation("Published telemetry message {Sequence}/{Count} to {Topic}", sequence, messageCount, telemetryTopic);

            if (sequence < messageCount && interval > TimeSpan.Zero)
            {
                await Task.Delay(interval, stoppingToken);
            }
        }

        await PublishControl(client, runId, challenge.TeamId, Topics.PublisherComplete, stoppingToken);
    }

    private static Task PublishControl(
        IMqttClient client,
        string runId,
        string teamId,
        string eventName,
        CancellationToken cancellationToken,
        int? deviceCount = null,
        int? messageIntervalMs = null)
    {
        var control = new ControlMessage(
            Topics.SchemaVersion,
            runId,
            teamId,
            eventName,
            DateTimeOffset.UtcNow)
        {
            DeviceCount = deviceCount,
            MessageIntervalMs = messageIntervalMs
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
