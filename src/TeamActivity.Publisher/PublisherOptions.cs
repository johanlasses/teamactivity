namespace TeamActivity.Publisher;

public sealed class PublisherOptions
{
    public int StartupDelaySeconds { get; init; } = 3;

    public int MessageCount { get; init; } = 1;

    public int MessageIntervalMilliseconds { get; init; } = 250;

    public int DeviceCount { get; init; } = 1;
}
