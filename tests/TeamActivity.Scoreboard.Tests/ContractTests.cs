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

    [Fact]
    public void RunMathScalesTheoreticalTelemetryByDeviceCount()
    {
        var total = RunMath.CalculateTheoreticalTelemetryCount(deviceCount: 3, runWindowSeconds: 120, intervalMs: 250);

        Assert.Equal(1_440, total);
    }

    [Fact]
    public void RunMathBuildsPerWindowExpectationsFromRunStart()
    {
        var runStart = DateTimeOffset.Parse("2026-05-07T20:17:34.900Z");
        var expectations = RunMath.BuildWindowExpectations(runStart, runWindowSeconds: 12, intervalMs: 250, windowSeconds: 5);

        Assert.Collection(
            expectations,
            window =>
            {
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:30Z"), window.WindowStartUtc);
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:35Z"), window.WindowEndUtc);
                Assert.Equal(1, window.ExpectedCountPerDevice);
            },
            window =>
            {
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:35Z"), window.WindowStartUtc);
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:40Z"), window.WindowEndUtc);
                Assert.Equal(20, window.ExpectedCountPerDevice);
            },
            window =>
            {
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:40Z"), window.WindowStartUtc);
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:45Z"), window.WindowEndUtc);
                Assert.Equal(20, window.ExpectedCountPerDevice);
            },
            window =>
            {
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:45Z"), window.WindowStartUtc);
                Assert.Equal(DateTimeOffset.Parse("2026-05-07T20:17:50Z"), window.WindowEndUtc);
                Assert.Equal(7, window.ExpectedCountPerDevice);
            });
    }

    [Fact]
    public void ScoreMathIntervalScoreIsMaxAtFiftyMs()
    {
        Assert.Equal(1000d, ScoreMath.CalculateIntervalScore(50));
    }

    [Fact]
    public void ScoreMathIntervalScoreIsZeroAtThousandMs()
    {
        Assert.Equal(0d, ScoreMath.CalculateIntervalScore(1000));
    }

    [Fact]
    public void ScoreMathIntervalScoreIsClampedToBoundsOutsideRange()
    {
        // better than best → clamped to 1000
        Assert.Equal(1000d, ScoreMath.CalculateIntervalScore(25));
        // worse than worst → clamped to 0
        Assert.Equal(0d, ScoreMath.CalculateIntervalScore(2000));
    }

    [Fact]
    public void ScoreMathIntervalScoreHitsAllAnchorPoints()
    {
        // Piecewise-linear anchors defined by the scoring spec
        Assert.Equal(1000d, ScoreMath.CalculateIntervalScore(50));
        Assert.Equal(800d,  ScoreMath.CalculateIntervalScore(100));
        Assert.Equal(500d,  ScoreMath.CalculateIntervalScore(250));
        Assert.Equal(250d,  ScoreMath.CalculateIntervalScore(500));
        Assert.Equal(100d,  ScoreMath.CalculateIntervalScore(750));
        Assert.Equal(0d,    ScoreMath.CalculateIntervalScore(1000));
    }

    [Fact]
    public void ScoreMathIntervalScoreInterpolatesBetweenAnchors()
    {
        // Midpoint of (50,1000)→(100,800) segment: 75 ms → 900
        Assert.InRange(ScoreMath.CalculateIntervalScore(75), 899d, 901d);
        // Midpoint of (250,500)→(500,250) segment: 375 ms → 375
        Assert.InRange(ScoreMath.CalculateIntervalScore(375), 374d, 376d);
    }

    [Fact]
    public void ScoreMathDeviceScoreIsMaxAtFiftyThousandDevices()
    {
        Assert.Equal(1000d, ScoreMath.CalculateDeviceScore(50_000));
    }

    [Fact]
    public void ScoreMathDeviceScoreHitsAllAnchorPoints()
    {
        // Log-linear anchors defined by the scoring spec
        Assert.InRange(ScoreMath.CalculateDeviceScore(10),     49d, 51d);
        Assert.InRange(ScoreMath.CalculateDeviceScore(100),    99d, 101d);
        Assert.InRange(ScoreMath.CalculateDeviceScore(1_000),  199d, 201d);
        Assert.InRange(ScoreMath.CalculateDeviceScore(10_000), 499d, 501d);
        Assert.Equal(1000d, ScoreMath.CalculateDeviceScore(50_000));
    }

    [Fact]
    public void ScoreMathDeviceScoreLinearSegmentAboveTenThousand()
    {
        // Midpoint of linear segment (10 000→50 000): 30 000 → 750
        Assert.InRange(ScoreMath.CalculateDeviceScore(30_000), 749d, 751d);
    }

    [Fact]
    public void ScoreMathDeviceScoreIsClampedAboveMaxDeviceCount()
    {
        Assert.Equal(1000d, ScoreMath.CalculateDeviceScore(100_000));
    }

    [Fact]
    public void ScoreMathLatencyScoreIsMaxAtHundredMs()
    {
        Assert.Equal(1000d, ScoreMath.CalculateLatencyScore(100));
    }

    [Fact]
    public void ScoreMathLatencyScoreIsZeroAtThousandMs()
    {
        Assert.Equal(0d, ScoreMath.CalculateLatencyScore(1000));
    }

    [Fact]
    public void ScoreMathLatencyScoreIsZeroWhenNoDataYet()
    {
        Assert.Equal(0d, ScoreMath.CalculateLatencyScore(0));
    }

    [Fact]
    public void ScoreMathLatencyScoreIsClampedToBoundsOutsideRange()
    {
        // better than best (< 100 ms) → clamped to 1000
        Assert.Equal(1000d, ScoreMath.CalculateLatencyScore(50));
        // worse than worst → clamped to 0
        Assert.Equal(0d, ScoreMath.CalculateLatencyScore(2000));
    }

    [Fact]
    public void ScoreMathLatencyScoreIsLinear()
    {
        // 550 ms → 1000 × (1000 − 550) / (1000 − 100) = 500 pts
        Assert.InRange(ScoreMath.CalculateLatencyScore(550), 499d, 501d);
    }

    [Fact]
    public void ScoreMathPublishAttainmentScoreScalesLinearly()
    {
        Assert.Equal(1000d, ScoreMath.CalculatePublishAttainmentScore(1.0));
        Assert.Equal(0d, ScoreMath.CalculatePublishAttainmentScore(0.0));
        Assert.InRange(ScoreMath.CalculatePublishAttainmentScore(0.5), 499d, 501d);
    }

    [Fact]
    public void ScoreMathPublishAttainmentScoreIsClamped()
    {
        Assert.Equal(1000d, ScoreMath.CalculatePublishAttainmentScore(1.5));
        Assert.Equal(0d, ScoreMath.CalculatePublishAttainmentScore(-0.1));
    }

    [Fact]
    public void ScoreMathWindowCorrectnessScoreScalesLinearly()
    {
        Assert.Equal(1000d, ScoreMath.CalculateWindowCorrectnessScore(1.0));
        Assert.Equal(0d, ScoreMath.CalculateWindowCorrectnessScore(0.0));
        Assert.InRange(ScoreMath.CalculateWindowCorrectnessScore(0.5), 499d, 501d);
    }

    [Fact]
    public void ScoreMathWindowCorrectnessScoreIsClamped()
    {
        Assert.Equal(1000d, ScoreMath.CalculateWindowCorrectnessScore(1.5));
        Assert.Equal(0d, ScoreMath.CalculateWindowCorrectnessScore(-0.1));
    }
}
