namespace TeamActivity.Shared.Contracts;

public sealed record TelemetryMessage(
    int SchemaVersion,
    string RunId,
    string TeamId,
    string DeviceId,
    long Sequence,
    DateTimeOffset EventTimeUtc,
    DateTimeOffset PublishedAtUtc,
    double Value);
