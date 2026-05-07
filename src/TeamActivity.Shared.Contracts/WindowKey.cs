namespace TeamActivity.Shared.Contracts;

public sealed record WindowKey(
    string RunId,
    string TeamId,
    string DeviceId,
    DateTimeOffset WindowStartUtc,
    DateTimeOffset WindowEndUtc);
