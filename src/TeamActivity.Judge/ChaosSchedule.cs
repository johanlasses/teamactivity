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
        new(15,  "start", "message-burst",      "High-frequency message burst"),
        new(35,  "end",   null,                 null),
        new(40,  "start", "device-dropout",     "Device stopped transmitting"),
        new(60,  "end",   null,                 null),
        new(65,  "start", "message-gap",        "Publisher paused — simulated connectivity loss"),
        new(80,  "end",   null,                 null),
        new(85,  "start", "high-latency",       "Network congestion injected"),
        new(100, "end",   null,                 null),
        new(105, "start", "processor-restart",  "Processor service restarted"),
        new(115, "end",   null,                 null),
    ];

    public static async Task RunAsync(
        ChaosStore chaos,
        ILogger logger,
        DateTimeOffset startedAtUtc,
        IEnumerable<ChaosScheduleEntry> schedule,
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
