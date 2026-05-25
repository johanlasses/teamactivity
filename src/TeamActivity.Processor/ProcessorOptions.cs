namespace TeamActivity.Processor;

public sealed class ProcessorOptions
{
    /// <summary>Number of partition workers. 0 = Environment.ProcessorCount.</summary>
    public int PartitionCount { get; init; } = 0;

    /// <summary>Bounded capacity per ingest partition channel.</summary>
    public int ChannelCapacity { get; init; } = 10_000;

    /// <summary>Number of MQTT connections for result publishing.</summary>
    public int PublishConnectionCount { get; init; } = 4;

    /// <summary>Bounded capacity of the shared result publish channel.</summary>
    public int PublishChannelCapacity { get; init; } = 50_000;

    /// <summary>How often each worker checks for due windows (ms).</summary>
    public int FlushCheckIntervalMs { get; init; } = 20;

    /// <summary>Grace period (ms) after windowEnd before flushing. Allows late-arriving messages to be included.</summary>
    public int WindowGraceMs { get; init; } = 50;

    public int ResolvedPartitionCount =>
        PartitionCount > 0 ? PartitionCount : Math.Max(2, Environment.ProcessorCount);
}
