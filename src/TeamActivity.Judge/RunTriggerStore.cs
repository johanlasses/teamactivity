namespace TeamActivity.Judge;

public sealed record RunTriggerConfig(
    string RunId,
    string Name,
    int DeviceCount,
    int IntervalMs,
    int RunWindowSeconds,
    bool ChaosEnabled,
    bool ChaosScheduleEnabled);

public sealed class RunTriggerStore
{
    public enum RunState { Idle, Pending, Running }

    private readonly object gate = new();
    private RunState state = RunState.Idle;
    private RunTriggerConfig? pending;
    private RunTriggerConfig? active;
    private DateTimeOffset? startedAtUtc;

    public bool TrySetPending(RunTriggerConfig config)
    {
        lock (gate)
        {
            if (state != RunState.Idle) return false;
            pending = config;
            state = RunState.Pending;
            return true;
        }
    }

    public RunTriggerConfig? GetPending()
    {
        lock (gate) return state == RunState.Pending ? pending : null;
    }

    public bool TryAcknowledge(string runId)
    {
        lock (gate)
        {
            if (state != RunState.Pending || pending?.RunId != runId) return false;
            active = pending;
            pending = null;
            startedAtUtc = DateTimeOffset.UtcNow;
            state = RunState.Running;
            return true;
        }
    }

    public bool TryCancel()
    {
        lock (gate)
        {
            if (state == RunState.Idle) return false;
            active = null;
            pending = null;
            startedAtUtc = null;
            state = RunState.Idle;
            return true;
        }
    }

    public void Complete(string runId)
    {
        lock (gate)
        {
            if (active?.RunId == runId)
            {
                active = null;
                startedAtUtc = null;
                state = RunState.Idle;
            }
        }
    }

    public RunStatusSnapshot GetStatus()
    {
        lock (gate) return new RunStatusSnapshot(state.ToString(), active ?? pending, startedAtUtc);
    }
}

public sealed record RunStatusSnapshot(string Status, RunTriggerConfig? Config, DateTimeOffset? StartedAtUtc);
