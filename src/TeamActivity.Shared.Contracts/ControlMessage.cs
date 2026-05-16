namespace TeamActivity.Shared.Contracts;

public sealed record ControlMessage(
    int SchemaVersion,
    string RunId,
    string TeamId,
    string Event,
    DateTimeOffset PublishedAtUtc)
{
    public int? DeviceCount { get; init; }

    public int? MessageIntervalMs { get; init; }

    public int? RunWindowSeconds { get; init; }
}
