namespace TeamActivity.Publisher;

public sealed class PublisherOptions
{
    public int StartupDelaySeconds { get; init; } = 3;
    public int ConnectionCount { get; init; } = 8;
    public int ShardCount { get; init; } = 64;
    public int ChannelCapacity { get; init; } = 4096;
}
