namespace TeamActivity.Publisher;

public sealed class PublisherOptions
{
    public int StartupDelaySeconds { get; init; } = 3;

    public int MessageIntervalMilliseconds { get; init; } = 250;

    public int DeviceCount { get; init; } = 1;

    /// <summary>
    /// Length of the run window in seconds. MessageCount is derived as RunWindowSeconds * 1000 / MessageIntervalMilliseconds.
    /// Default is 120 (2 minutes). Override to a smaller value in CI/smoke-test environments for a faster run.
    /// </summary>
    public int RunWindowSeconds { get; init; } = 120;
}
