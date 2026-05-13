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
                chaos.StartEvent(entry.EventType, entry.Description ?? string.Empty);
            else if (entry.Action == "end")
                chaos.EndEvent();
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
