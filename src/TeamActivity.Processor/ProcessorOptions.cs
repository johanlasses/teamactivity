namespace TeamActivity.Processor;

public sealed class ProcessorOptions
{
    /// <summary>
    /// How long after a window's end to wait before publishing its aggregate. A small delay lets
    /// any in-flight telemetry (network/publisher jitter) arrive so the Processor's count matches
    /// the Judge's, while staying well inside the latency budget.
    /// </summary>
    public int PublishDelayMs { get; init; } = 100;

    /// <summary>How often to check for windows that are due to be published.</summary>
    public int PollIntervalMs { get; init; } = 50;

    /// <summary>Delay between MQTT reconnect attempts after an unexpected disconnect (e.g. chaos).</summary>
    public int ReconnectDelayMs { get; init; } = 100;
}
