namespace TeamActivity.Processor;

public sealed class ProcessorOptions
{
    /// <summary>Number of partition workers. 0 = Environment.ProcessorCount.</summary>
    public int PartitionCount { get; init; } = 0;

    /// <summary>Number of shared-subscription MQTT connections for receiving telemetry.</summary>
    public int SubscribeConnectionCount { get; init; } = 16;

    /// <summary>Number of MQTT connections for result publishing.</summary>
    public int PublishConnectionCount { get; init; } = 32;

    /// <summary>Bounded capacity of the shared result publish channel.</summary>
    public int PublishChannelCapacity { get; init; } = 50_000;

    /// <summary>How often each worker checks for due windows (ms).</summary>
    public int FlushCheckIntervalMs { get; init; } = 5;

    /// <summary>Grace period (ms) after windowEnd before flushing.</summary>
    public int WindowGraceMs { get; init; } = 50;

    public int ResolvedPartitionCount =>
        PartitionCount > 0 ? PartitionCount : Math.Max(2, Environment.ProcessorCount);
}
