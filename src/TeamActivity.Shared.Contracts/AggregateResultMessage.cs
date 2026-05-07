namespace TeamActivity.Shared.Contracts;

public sealed record AggregateResultMessage(
    int SchemaVersion,
    string RunId,
    string TeamId,
    string DeviceId,
    DateTimeOffset WindowStartUtc,
    DateTimeOffset WindowEndUtc,
    int Count,
    double Sum,
    double Min,
    double Max,
    double Avg,
    string ResultId,
    DateTimeOffset PublishedAtUtc);
