namespace TeamActivity.Shared.Contracts;

public sealed class ChallengeOptions
{
    public string RunId { get; init; } = "run-template";

    public string TeamId { get; init; } = "team-template";

    public int WindowSeconds { get; init; } = 5;

    public int GraceSeconds { get; init; } = 2;
}
