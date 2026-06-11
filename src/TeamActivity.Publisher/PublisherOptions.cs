namespace TeamActivity.Publisher;

public sealed class PublisherOptions
{
    public int StartupDelaySeconds { get; init; } = 3;
    public int ShardCount { get; init; } = 8;
    public int MaxInFlightPerShard { get; init; } = 1024;
}
