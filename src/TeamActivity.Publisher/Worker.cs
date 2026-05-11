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
    IOptions<PublisherOptions> publisherOptions) : BackgroundService
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

        var intervalMs = publisherOptions.Value.MessageIntervalMilliseconds;
        if (intervalMs <= 0)
        {
            throw new InvalidOperationException(
                $"Publisher:MessageIntervalMilliseconds must be greater than 0, but was {intervalMs}.");
        }

        var deviceCount = Math.Max(1, publisherOptions.Value.DeviceCount);
        var runWindowMs = Math.Max(1, publisherOptions.Value.RunWindowSeconds) * 1000;
        var messageCount = Math.Max(1, runWindowMs / intervalMs);
        var interval = TimeSpan.FromMilliseconds(intervalMs);

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithClientId($"publisher-{challenge.TeamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        logger.LogInformation("Connecting publisher to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        await client.ConnectAsync(options, stoppingToken);

        await PublishControl(client, challenge, Topics.PublisherStart, stoppingToken, deviceCount, intervalMs);

        var telemetryTopic = Topics.TelemetryRaw(challenge.RunId, challenge.TeamId);

        for (var sequence = 1; sequence <= messageCount; sequence++)
        {
            var deviceNumber = (sequence - 1) % deviceCount + 1;
            var now = DateTimeOffset.UtcNow;
            var telemetry = new TelemetryMessage(
                Topics.SchemaVersion,
                challenge.RunId,
                challenge.TeamId,
                $"device-{deviceNumber:000}",
                sequence,
                now,
                now,
                40 + deviceNumber + sequence / 10.0);

            var telemetryJson = JsonSerializer.Serialize(telemetry, JsonContract.Options);
            await PublishJson(client, telemetryTopic, telemetryJson, stoppingToken);
            TelemetryPublished.Add(1, new KeyValuePair<string, object?>("team_id", challenge.TeamId));
            logger.LogInformation("Published telemetry message {Sequence}/{Count} to {Topic}: {Payload}", sequence, messageCount, telemetryTopic, telemetryJson);

            if (sequence < messageCount && interval > TimeSpan.Zero)
            {
                await Task.Delay(interval, stoppingToken);
            }
        }

        await PublishControl(client, challenge, Topics.PublisherComplete, stoppingToken);

        logger.LogInformation("Publisher finished the starter message flow and will stay alive for inspection.");
        await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
    }

    private static Task PublishControl(
        IMqttClient client,
        ChallengeOptions challenge,
        string eventName,
        CancellationToken cancellationToken,
        int? deviceCount = null,
        int? messageIntervalMs = null)
    {
        var control = new ControlMessage(
            Topics.SchemaVersion,
            challenge.RunId,
            challenge.TeamId,
            eventName,
            DateTimeOffset.UtcNow)
        {
            DeviceCount = deviceCount,
            MessageIntervalMs = messageIntervalMs
        };

        var topic = Topics.Control(challenge.RunId, challenge.TeamId, eventName);
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
