namespace TeamActivity.Judge;

public sealed class ChaosStore
{
    public static readonly IReadOnlySet<string> AllowedEventTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "message-duplications",
        "publisher-disconnect",
        "processor-disconnect"
    };

    private readonly object gate = new();
    private ChaosState state = new(false, null, null);

    public ChaosState GetState()
    {
        lock (gate) return state;
    }

    public void Enable(string runId)
    {
        lock (gate)
        {
            state = new ChaosState(true, runId, state.ActiveEvent);
        }
    }

    public void Disable()
    {
        lock (gate)
        {
            state = new ChaosState(false, null, null);
        }
    }

    public void StartEvent(string eventType, string description)
    {
        lock (gate)
        {
            var ev = new ActiveChaosEvent(Guid.NewGuid().ToString(), eventType, description, DateTimeOffset.UtcNow);
            state = new ChaosState(state.Enabled, state.RunId, ev);
        }
    }

    public void EndEvent()
    {
        lock (gate)
        {
            state = new ChaosState(state.Enabled, state.RunId, null);
        }
    }
}

public sealed record ChaosState(
    bool Enabled,
    string? RunId,
    ActiveChaosEvent? ActiveEvent);

public sealed record ActiveChaosEvent(
    string Id,
    string Type,
    string Description,
    DateTimeOffset StartedAtUtc);
