using TeamActivity.Shared.Contracts;

namespace TeamActivity.Scoreboard.Tests;

public sealed class ContractTests
{
    [Fact]
    public void WindowMathAssignsEventsToFiveSecondUnixEpochWindows()
    {
        var telemetry = new TelemetryMessage(
            Topics.SchemaVersion,
            "run-1",
            "team-1",
            "device-1",
            10,
            DateTimeOffset.Parse("2026-05-07T20:17:34.999Z"),
            DateTimeOffset.Parse("2026-05-07T20:17:35.100Z"),
            12.5);

        var window = WindowMath.Assign(telemetry, 5);

        Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:30Z"), window.WindowStartUtc);
        Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:35Z"), window.WindowEndUtc);
    }

    [Fact]
    public void AggregateWindowMatchesEquivalentResultPayload()
    {
        var key = new WindowKey(
            "run-1",
            "team-1",
            "device-1",
            DateTimeOffset.Parse("2026-05-07T20:17:30Z"),
            DateTimeOffset.Parse("2026-05-07T20:17:35Z"));

        var aggregate = new AggregateWindow();
        aggregate.Add(10);
        aggregate.Add(20);

        var result = new AggregateResultMessage(
            Topics.SchemaVersion,
            key.RunId,
            key.TeamId,
            key.DeviceId,
            key.WindowStartUtc,
            key.WindowEndUtc,
            2,
            30,
            10,
            20,
            15,
            WindowMath.ResultId(key),
            DateTimeOffset.UtcNow);

        Assert.True(aggregate.Matches(result));
    }

    [Fact]
    public void ResultTopicRoundTripsThroughParser()
    {
        var windowStart = DateTimeOffset.Parse("2026-05-07T20:17:30Z");
        var topic = Topics.Result("run-1", "team-1", "device-1", windowStart);

        var parsed = Topics.TryParseResult(topic, out var runId, out var teamId, out var deviceId, out var windowStartUnixMs);

        Assert.True(parsed);
        Assert.Equal("run-1", runId);
        Assert.Equal("team-1", teamId);
        Assert.Equal("device-1", deviceId);
        Assert.Equal(windowStart.ToUnixTimeMilliseconds(), windowStartUnixMs);
    }
}
