namespace TeamActivity.Shared.Contracts;

public sealed record RunTriggerMessage(
    string RunId,
    int DeviceCount,
    int IntervalMs,
    int RunWindowSeconds,
    bool ChaosEnabled);
