namespace TeamActivity.Publisher;

public sealed class PublisherOptions
{
    public int StartupDelaySeconds { get; init; } = 3;
    public int ConnectionCount { get; init; } = 16;
    public int ShardCount { get; init; } = 64;
    public int ChannelCapacity { get; init; } = 8192;
    public int DrainTimeoutSeconds { get; init; } = 10;
    public int MaxPublishRetryAttempts { get; init; } = 5;
}
