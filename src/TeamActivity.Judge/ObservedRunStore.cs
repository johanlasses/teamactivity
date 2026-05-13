namespace TeamActivity.Judge;

public sealed class ObservedRunStore
{
    private const int MaxMessagesPerRun = 500;

    private readonly object gate = new();
    private readonly Dictionary<string, List<ObservedMessage>> messagesByRun = [];
    private readonly Dictionary<string, string> runNames = [];

    public void RegisterRunName(string runId, string name)
    {
        lock (gate) runNames[runId] = name;
    }

    public void AddMessage(ObservedMessage message)
    {
        lock (gate)
        {
            if (!messagesByRun.TryGetValue(message.RunId, out var messages))
            {
                messages = [];
                messagesByRun.Add(message.RunId, messages);
            }

            messages.Add(message);
            if (messages.Count > MaxMessagesPerRun)
            {
                messages.RemoveRange(0, messages.Count - MaxMessagesPerRun);
            }
        }
    }

    public IReadOnlyList<RunSnapshot> GetRuns()
    {
        lock (gate)
        {
            return messagesByRun
                .Select(entry =>
                {
                    var messages = entry.Value;
                    var name = runNames.GetValueOrDefault(entry.Key, entry.Key);
                    return new RunSnapshot(
                        entry.Key,
                        name,
                        messages.Select(message => message.TeamId).Distinct().Order().ToArray(),
                        messages.Count,
                        messages.Count(message => message.IsValid),
                        messages.Count(message => !message.IsValid),
                        messages.Max(message => message.ReceivedAtUtc));
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
                ? messages.OrderBy(message => message.ReceivedAtUtc).ToArray()
                : [];
        }
    }
}

public sealed record RunSnapshot(
    string RunId,
    string Name,
    IReadOnlyList<string> TeamIds,
    int MessageCount,
    int ValidMessageCount,
    int InvalidMessageCount,
    DateTimeOffset LastUpdatedUtc);
