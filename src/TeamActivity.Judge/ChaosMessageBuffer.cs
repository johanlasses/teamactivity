namespace TeamActivity.Judge;

public sealed class ChaosMessageBuffer
{
    private readonly object gate = new();
    private readonly List<(string Topic, string Payload, DateTimeOffset ReceivedAt)> buffer = [];

    public void Add(string topic, string payload)
    {
        lock (gate)
        {
            buffer.Add((topic, payload, DateTimeOffset.UtcNow));
            // Trim entries older than 30s to keep memory bounded
            var cutoff = DateTimeOffset.UtcNow.AddSeconds(-30);
            buffer.RemoveAll(m => m.ReceivedAt < cutoff);
        }
    }

    public IReadOnlyList<(string Topic, string Payload)> GetRecent(TimeSpan window)
    {
        lock (gate)
        {
            var cutoff = DateTimeOffset.UtcNow - window;
            return buffer
                .Where(m => m.ReceivedAt >= cutoff)
                .Select(m => (m.Topic, m.Payload))
                .ToList();
        }
    }
}
