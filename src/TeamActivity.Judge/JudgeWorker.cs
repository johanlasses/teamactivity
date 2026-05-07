using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Judge;

public sealed class JudgeWorker(
    ILogger<JudgeWorker> logger,
    IOptions<MqttOptions> mqttOptions,
    IOptions<ChallengeOptions> challengeOptions,
    ScoringStore scoring,
    ObservedRunStore store) : BackgroundService
{
    private static readonly Counter<long> RawReceived = TelemetryMeters.Judge.CreateCounter<long>("judge_raw_received_total");
    private static readonly Counter<long> ResultsReceived = TelemetryMeters.Judge.CreateCounter<long>("judge_results_received_total");
    private static readonly Counter<long> ControlReceived = TelemetryMeters.Judge.CreateCounter<long>("judge_control_received_total");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqtt = mqttOptions.Value;

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();

        client.ApplicationMessageReceivedAsync += args =>
        {
            HandleMessage(args.ApplicationMessage.Topic, Encoding.UTF8.GetString(args.ApplicationMessage.Payload), DateTimeOffset.UtcNow);
            return Task.CompletedTask;
        };

        var options = new MqttClientOptionsBuilder()
            .WithClientId("judge")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();

        logger.LogInformation("Connecting judge to MQTT broker {Host}:{Port}", mqtt.Host, mqtt.Port);
        await client.ConnectAsync(options, stoppingToken);

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(filter => filter
                .WithTopic("telemetry/v1/+/+/raw")
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
            .WithTopicFilter(filter => filter
                .WithTopic("control/v1/+/+/+")
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .WithTopicFilter(filter => filter
                .WithTopic("results/v1/+/+/device/+/window/+")
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build();

        await client.SubscribeAsync(subscribeOptions, stoppingToken);
        logger.LogInformation("Judge subscribed to telemetry, result, and control topics.");

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            scoring.FinalizeDueWindows(challengeOptions.Value.GraceSeconds);
        }
    }

    private void HandleMessage(string topic, string payload, DateTimeOffset receivedAtUtc)
    {
        if (Topics.TryParseTelemetryRaw(topic, out var telemetryRunId, out var telemetryTeamId))
        {
            HandleTelemetry(topic, payload, telemetryRunId, telemetryTeamId);
            return;
        }

        if (Topics.TryParseControl(topic, out var controlRunId, out var controlTeamId, out var eventName))
        {
            HandleControl(topic, payload, controlRunId, controlTeamId, eventName);
            return;
        }

        if (Topics.TryParseResult(topic, out var resultRunId, out var resultTeamId, out var resultDeviceId, out var windowStartUnixMs))
        {
            HandleResult(topic, payload, resultRunId, resultTeamId, resultDeviceId, windowStartUnixMs, receivedAtUtc);
            return;
        }

        logger.LogWarning("Judge ignored unsupported MQTT topic {Topic}", topic);
    }

    private void HandleTelemetry(string topic, string payload, string topicRunId, string topicTeamId)
    {
        try
        {
            var telemetry = JsonSerializer.Deserialize<TelemetryMessage>(payload, JsonContract.Options);
            var error = ValidateTelemetry(telemetry, topicRunId, topicTeamId);

            store.AddMessage(new ObservedMessage(
                topicRunId,
                topicTeamId,
                "telemetry",
                topic,
                payload,
                error is null,
                error,
                DateTimeOffset.UtcNow));

            if (telemetry is not null && error is null)
            {
                scoring.ObserveTelemetry(telemetry, challengeOptions.Value.WindowSeconds);
                RawReceived.Add(1, new KeyValuePair<string, object?>("team_id", telemetry.TeamId));
            }

            logger.LogInformation("Judge observed telemetry on {Topic}. Valid={Valid}", topic, error is null);
        }
        catch (JsonException ex)
        {
            store.AddMessage(new ObservedMessage(topicRunId, topicTeamId, "telemetry", topic, payload, false, ex.Message, DateTimeOffset.UtcNow));
            logger.LogWarning(ex, "Judge failed to deserialize telemetry payload on {Topic}", topic);
        }
    }

    private void HandleControl(string topic, string payload, string topicRunId, string topicTeamId, string eventName)
    {
        try
        {
            var control = JsonSerializer.Deserialize<ControlMessage>(payload, JsonContract.Options);
            var error = ValidateControl(control, topicRunId, topicTeamId, eventName);

            store.AddMessage(new ObservedMessage(
                topicRunId,
                topicTeamId,
                "control",
                topic,
                payload,
                error is null,
                error,
                DateTimeOffset.UtcNow));

            if (control is not null && error is null)
            {
                scoring.ObserveControl(control);
                ControlReceived.Add(1, new KeyValuePair<string, object?>("event", control.Event));
            }

            logger.LogInformation("Judge observed control event {Event} on {Topic}. Valid={Valid}", eventName, topic, error is null);
        }
        catch (JsonException ex)
        {
            store.AddMessage(new ObservedMessage(topicRunId, topicTeamId, "control", topic, payload, false, ex.Message, DateTimeOffset.UtcNow));
            logger.LogWarning(ex, "Judge failed to deserialize control payload on {Topic}", topic);
        }
    }

    private void HandleResult(
        string topic,
        string payload,
        string topicRunId,
        string topicTeamId,
        string topicDeviceId,
        long topicWindowStartUnixMs,
        DateTimeOffset receivedAtUtc)
    {
        try
        {
            var result = JsonSerializer.Deserialize<AggregateResultMessage>(payload, JsonContract.Options);
            var error = ValidateResult(result, topicRunId, topicTeamId, topicDeviceId, topicWindowStartUnixMs);

            store.AddMessage(new ObservedMessage(
                topicRunId,
                topicTeamId,
                "result",
                topic,
                payload,
                error is null,
                error,
                receivedAtUtc));

            if (result is not null && error is null)
            {
                scoring.ObserveResult(result, receivedAtUtc);
                ResultsReceived.Add(1, new KeyValuePair<string, object?>("team_id", result.TeamId));
            }
            else
            {
                scoring.AddInvalid(topicRunId, topicTeamId);
            }

            logger.LogInformation("Judge observed aggregate result on {Topic}. Valid={Valid}", topic, error is null);
        }
        catch (JsonException ex)
        {
            store.AddMessage(new ObservedMessage(topicRunId, topicTeamId, "result", topic, payload, false, ex.Message, receivedAtUtc));
            scoring.AddInvalid(topicRunId, topicTeamId);
            logger.LogWarning(ex, "Judge failed to deserialize aggregate result payload on {Topic}", topic);
        }
    }

    private static string? ValidateTelemetry(TelemetryMessage? telemetry, string topicRunId, string topicTeamId)
    {
        if (telemetry is null)
        {
            return "Payload did not contain a telemetry message.";
        }

        if (telemetry.SchemaVersion != Topics.SchemaVersion)
        {
            return $"Unsupported schemaVersion {telemetry.SchemaVersion}.";
        }

        if (telemetry.RunId != topicRunId || telemetry.TeamId != topicTeamId)
        {
            return "Payload runId/teamId did not match the MQTT topic.";
        }

        if (string.IsNullOrWhiteSpace(telemetry.DeviceId))
        {
            return "deviceId is required.";
        }

        return null;
    }

    private static string? ValidateControl(ControlMessage? control, string topicRunId, string topicTeamId, string eventName)
    {
        if (control is null)
        {
            return "Payload did not contain a control message.";
        }

        if (control.SchemaVersion != Topics.SchemaVersion)
        {
            return $"Unsupported schemaVersion {control.SchemaVersion}.";
        }

        if (control.RunId != topicRunId || control.TeamId != topicTeamId || control.Event != eventName)
        {
            return "Payload runId/teamId/event did not match the MQTT topic.";
        }

        return eventName is Topics.PublisherStart or Topics.PublisherComplete
            ? null
            : $"Unsupported control event {eventName}.";
    }

    private static string? ValidateResult(
        AggregateResultMessage? result,
        string topicRunId,
        string topicTeamId,
        string topicDeviceId,
        long topicWindowStartUnixMs)
    {
        if (result is null)
        {
            return "Payload did not contain an aggregate result message.";
        }

        if (result.SchemaVersion != Topics.SchemaVersion)
        {
            return $"Unsupported schemaVersion {result.SchemaVersion}.";
        }

        if (result.RunId != topicRunId || result.TeamId != topicTeamId || result.DeviceId != topicDeviceId)
        {
            return "Payload runId/teamId/deviceId did not match the MQTT topic.";
        }

        if (result.WindowStartUtc.ToUnixTimeMilliseconds() != topicWindowStartUnixMs)
        {
            return "Payload windowStartUtc did not match the MQTT topic.";
        }

        if (result.WindowEndUtc <= result.WindowStartUtc)
        {
            return "windowEndUtc must be after windowStartUtc.";
        }

        return null;
    }
}
