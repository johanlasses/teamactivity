namespace TeamActivity.Publisher;

public sealed class PublisherOptions
{
    public int StartupDelaySeconds { get; init; } = 3;

    /// <summary>Delay between MQTT reconnect attempts after an unexpected disconnect (e.g. chaos).</summary>
    public int ReconnectDelayMs { get; init; } = 100;
}
