using TeamActivity.Shared.Contracts;

namespace TeamActivity.Judge;

public sealed class ObservedRunStore
{
    private const int MaxMessagesPerRun = 500;

    private readonly object gate = new();
    private readonly Dictionary<string, Queue<ObservedMessage>> messagesByRun = [];
    private readonly Dictionary<string, string> runNames = [];
    private readonly Dictionary<string, RunTrafficState> runTrafficByRun = [];

    public void RegisterRunName(string runId, string name)
    {
        lock (gate) runNames[runId] = name;
    }

    public void RegisterRunParameters(string runId, int deviceCount, int messageIntervalMs, int runWindowSeconds)
    {
        lock (gate)
        {
            var traffic = GetOrCreateTraffic(runId);
            traffic.TheoreticalTelemetryCount = RunMath.CalculateTheoreticalTelemetryCount(deviceCount, runWindowSeconds, messageIntervalMs);
        }
    }

    public void AddMessage(ObservedMessage message)
    {
        lock (gate)
        {
            if (!messagesByRun.TryGetValue(message.RunId, out var messages))
            {
                messages = new Queue<ObservedMessage>();
                messagesByRun.Add(message.RunId, messages);
            }

            messages.Enqueue(message);
            // O(1) dequeue from the front — no element shifting compared to List.RemoveRange.
            while (messages.Count > MaxMessagesPerRun)
                messages.Dequeue();

            var traffic = GetOrCreateTraffic(message.RunId);
            traffic.TeamIds.Add(message.TeamId);
            traffic.MessageCount++;
            traffic.LastUpdatedUtc = message.ReceivedAtUtc;

            if (message.IsValid)
            {
                traffic.ValidMessageCount++;
            }
            else
            {
                traffic.InvalidMessageCount++;
            }

            switch (message.Kind)
            {
                case "telemetry":
                    traffic.TelemetryMessageCount++;
                    break;
                case "result":
                    traffic.ResultMessageCount++;
                    break;
                case "control":
                    traffic.ControlMessageCount++;
                    break;
            }
        }
    }

    public IReadOnlyList<RunSnapshot> GetRuns()
    {
        lock (gate)
        {
            return runTrafficByRun
                .Select(entry =>
                {
                    var name = runNames.GetValueOrDefault(entry.Key, entry.Key);
                    var traffic = entry.Value;
                    var publishAttainment = traffic.TheoreticalTelemetryCount > 0
                        ? Math.Min(1d, (double)traffic.TelemetryMessageCount / traffic.TheoreticalTelemetryCount.Value)
                        : 0d;
                    return new RunSnapshot(
                        entry.Key,
                        name,
                        traffic.TeamIds.Order().ToArray(),
                        traffic.MessageCount,
                        traffic.ValidMessageCount,
                        traffic.InvalidMessageCount,
                        traffic.LastUpdatedUtc,
                        traffic.TheoreticalTelemetryCount,
                        traffic.TelemetryMessageCount,
                        publishAttainment,
                        traffic.ResultMessageCount,
                        traffic.ControlMessageCount);
                })
                .OrderBy(snapshot => snapshot.RunId)
                .ToArray();
        }
    }

    public IReadOnlyList<ObservedMessage> GetMessages(string runId)
    {
        lock (gate)
        {
            return messagesByRun.TryGetValue(runId, out var messages)
                ? messages.ToArray()
                : [];
        }
    }

    private RunTrafficState GetOrCreateTraffic(string runId)
    {
        if (!runTrafficByRun.TryGetValue(runId, out var traffic))
        {
            traffic = new RunTrafficState();
            runTrafficByRun.Add(runId, traffic);
        }

        return traffic;
    }
}

public sealed record RunSnapshot(
    string RunId,
    string Name,
    IReadOnlyList<string> TeamIds,
    long MessageCount,
    long ValidMessageCount,
    long InvalidMessageCount,
    DateTimeOffset LastUpdatedUtc,
    long? TheoreticalTelemetryCount,
    long TelemetryMessageCount,
    double PublishAttainment,
    long ResultMessageCount,
    long ControlMessageCount);

sealed class RunTrafficState
{
    public HashSet<string> TeamIds { get; } = [];

    public long MessageCount { get; set; }

    public long ValidMessageCount { get; set; }

    public long InvalidMessageCount { get; set; }

    public long TelemetryMessageCount { get; set; }

    public long ResultMessageCount { get; set; }

    public long ControlMessageCount { get; set; }

    public long? TheoreticalTelemetryCount { get; set; }

    public DateTimeOffset LastUpdatedUtc { get; set; } = DateTimeOffset.UtcNow;
}
