namespace TeamActivity.Processor;

public sealed class ProcessorOptions
{
    public int PublishConnectionCount { get; init; } = 32;
    public int PublishChannelCapacity { get; init; } = 50_000;
    public int FlushCheckIntervalMs { get; init; } = 5;
    public int WindowGraceMs { get; init; } = 50;
}
