namespace TeamActivity.Publisher;

public sealed class PublisherOptions
{
    public int StartupDelaySeconds { get; init; } = 3;

    public string CheckpointFileName { get; init; } = "publisher-active-run.json";

    public int CheckpointEveryEmissions { get; init; } = 4;

    public int ReconnectInitialDelayMs { get; init; } = 100;

    public int ReconnectMaxDelayMs { get; init; } = 5000;

    public int IdlePollIntervalMs { get; init; } = 1000;
}
