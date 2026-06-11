namespace TeamActivity.Processor;

public sealed class ProcessorOptions
{
    /// <summary>Number of parallel channel partitions for telemetry processing.</summary>
    public int PartitionCount { get; init; } = 8;

    /// <summary>Milliseconds after windowEnd before publishing the aggregate result (latency tuning).</summary>
    public int PublishDelayMs { get; init; } = 50;

    /// <summary>Maximum concurrent in-flight QoS-1 result publishes.</summary>
    public int MaxInFlightResults { get; init; } = 256;
}
