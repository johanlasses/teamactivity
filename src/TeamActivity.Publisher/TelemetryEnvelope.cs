namespace TeamActivity.Publisher;

public readonly record struct TelemetryEnvelope(
    string RunId,
    string TeamId,
    string DeviceId,
    long Sequence,
    DateTimeOffset EventTimeUtc,
    DateTimeOffset PublishedAtUtc,
    double Value);
