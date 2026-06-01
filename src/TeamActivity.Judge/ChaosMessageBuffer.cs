namespace TeamActivity.Judge;

public sealed class ChaosMessageBuffer
{
    private readonly object gate = new();
    private readonly Queue<(string Topic, string Payload, DateTimeOffset ReceivedAt)> buffer = new();

    public void Add(string topic, string payload)
    {
        lock (gate)
        {
            buffer.Enqueue((topic, payload, DateTimeOffset.UtcNow));
            // Trim entries older than 30s from the front — O(k) dequeues instead of O(n) RemoveAll.
            var cutoff = DateTimeOffset.UtcNow.AddSeconds(-30);
            while (buffer.Count > 0 && buffer.Peek().ReceivedAt < cutoff)
                buffer.Dequeue();
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
