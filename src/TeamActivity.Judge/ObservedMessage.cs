namespace TeamActivity.Judge;

public sealed record ObservedMessage(
    string RunId,
    string TeamId,
    string Kind,
    string Topic,
    string Payload,
    bool IsValid,
    string? Error,
    DateTimeOffset ReceivedAtUtc);
