using MQTTnet;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Judge;

public sealed record ChaosScheduleEntry(
    int OffsetSeconds,
    string Action,
    string? EventType,
    string? Description);

public static class ChaosSchedule
{
    public static readonly IReadOnlyList<ChaosScheduleEntry> DefaultSchedule =
    [
        new(20,  "start", "message-duplications",  "Duplicate messages injected"),
        new(30,  "end",   null,                     null),
        new(60,  "start", "publisher-disconnect",   "Forcing publisher to reconnect"),
        new(65,  "end",   null,                     null),
        new(90,  "start", "processor-disconnect",   "Forcing processor to reconnect"),
        new(95,  "end",   null,                     null),
    ];

    public static async Task RunAsync(
        ChaosStore chaos,
        ILogger logger,
        DateTimeOffset startedAtUtc,
        IEnumerable<ChaosScheduleEntry> schedule,
        ChaosMessageBuffer buffer,
        RunAnnouncer announcer,
        MqttOptions mqttOptions,
        string teamId,
        CancellationToken cancellationToken)
    {
        foreach (var entry in schedule.OrderBy(e => e.OffsetSeconds))
        {
            var fireAt = startedAtUtc.AddSeconds(entry.OffsetSeconds);
            var delay = fireAt - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero)
            {
                try { await Task.Delay(delay, cancellationToken); }
                catch (OperationCanceledException) { break; }
            }

            if (cancellationToken.IsCancellationRequested) break;

            if (entry.Action == "start" && entry.EventType is not null)
            {
                chaos.StartEvent(entry.EventType, entry.Description ?? string.Empty);
                logger.LogInformation(
                    "Chaos event started: Type={EventType} Description={Description}",
                    entry.EventType, entry.Description);

                await ExecuteEventActionAsync(entry.EventType, buffer, announcer, mqttOptions, teamId, logger, cancellationToken);
            }
            else if (entry.Action == "end")
            {
                var ending = chaos.GetState().ActiveEvent;
                chaos.EndEvent();
                if (ending is not null)
                {
                    var duration = DateTimeOffset.UtcNow - ending.StartedAtUtc;
                    logger.LogInformation(
                        "Chaos event ended: Type={EventType} Duration={DurationSeconds:F1}s",
                        ending.Type, duration.TotalSeconds);
                }
            }
        }
    }

    public static Task ExecuteManualEventActionAsync(
        string eventType,
        ChaosMessageBuffer buffer,
        RunAnnouncer announcer,
        MqttOptions mqttOptions,
        string teamId,
        ILogger logger) =>
        ExecuteEventActionAsync(eventType, buffer, announcer, mqttOptions, teamId, logger, CancellationToken.None);

    private static async Task ExecuteEventActionAsync(
        string eventType,
        ChaosMessageBuffer buffer,
        RunAnnouncer announcer,
        MqttOptions mqttOptions,
        string teamId,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        switch (eventType)
        {
            case "message-duplications":
                await RunMessageBurstAsync(buffer, announcer, logger, cancellationToken);
                break;

            case "publisher-disconnect":
                await DisconnectClientAsync($"publisher-{teamId}", mqttOptions, logger, cancellationToken);
                break;

            case "processor-disconnect":
                await DisconnectClientAsync($"processor-{teamId}", mqttOptions, logger, cancellationToken);
                break;
        }
    }

    private static async Task RunMessageBurstAsync(
        ChaosMessageBuffer buffer,
        RunAnnouncer announcer,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var messages = buffer.GetRecent(TimeSpan.FromSeconds(3));
        if (messages.Count == 0)
        {
            logger.LogWarning("Chaos message-duplications: no recent messages in buffer, skipping replay");
            return;
        }

        logger.LogInformation("Chaos message-duplications: replaying {Count} recent messages", messages.Count);
        foreach (var (topic, payload) in messages)
        {
            if (cancellationToken.IsCancellationRequested) break;
            announcer.Announce(topic, payload);
            await Task.Delay(5, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task DisconnectClientAsync(
        string clientId,
        MqttOptions mqttOptions,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        try
        {
            var factory = new MqttClientFactory();
            using var impersonator = factory.CreateMqttClient();
            var options = new MqttClientOptionsBuilder()
                .WithClientId(clientId)
                .WithTcpServer(mqttOptions.Host, mqttOptions.Port)
                .WithCleanStart()
                .Build();

            // Connecting with the same client ID forces the broker to disconnect the real client
            await impersonator.ConnectAsync(options, cancellationToken);
            await impersonator.DisconnectAsync(cancellationToken: cancellationToken);
            logger.LogInformation("Chaos disconnect: evicted client {ClientId}", clientId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Chaos disconnect failed for client {ClientId}", clientId);
        }
    }
}

/// <summary>Tracks per-run CancellationTokenSources for the chaos schedule runner.</summary>
public sealed class ChaosScheduleTracker
{
    private readonly object gate = new();
    private readonly Dictionary<string, CancellationTokenSource> byRunId = [];

    public void Register(string runId, CancellationTokenSource cts)
    {
        lock (gate) byRunId[runId] = cts;
    }

    public void Cancel(string runId)
    {
        lock (gate)
        {
            if (byRunId.TryGetValue(runId, out var cts))
            {
                cts.Cancel();
                byRunId.Remove(runId);
            }
        }
    }
}
