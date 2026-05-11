namespace TeamActivity.Shared.Contracts;

public sealed record ScoreSnapshot(
    string RunId,
    string TeamId,
    int Correct,
    int Invalid,
    int Missing,
    double LatencyP95Ms,
    double Score,
    DateTimeOffset LastUpdatedUtc,
    string Status,
    int? DeviceCount,
    int? MessageIntervalMs);
