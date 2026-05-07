namespace TeamActivity.Shared.Contracts;

public sealed record ControlMessage(
    int SchemaVersion,
    string RunId,
    string TeamId,
    string Event,
    DateTimeOffset PublishedAtUtc);
