using System.Diagnostics.Metrics;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Judge;

public sealed class ScoringStore
{
    private static readonly TimeSpan FinalizationSlack = TimeSpan.FromSeconds(1);
    private static readonly Counter<long> Correct = TelemetryMeters.Judge.CreateCounter<long>("judge_correct_total");
    private static readonly Counter<long> Invalid = TelemetryMeters.Judge.CreateCounter<long>("judge_invalid_total");
    private static readonly Counter<long> Missing = TelemetryMeters.Judge.CreateCounter<long>("judge_missing_total");

    private readonly object gate = new();
    private readonly Dictionary<WindowKey, ExpectedWindow> expectedWindows = [];
    private readonly Dictionary<WindowKey, ResultCandidate> resultCandidates = [];
    private readonly HashSet<string> seenTelemetry = [];
    private readonly HashSet<WindowKey> finalizedWindows = [];
    private readonly Dictionary<(string RunId, string TeamId), ScoreState> scores = [];
    private readonly Dictionary<(string RunId, string TeamId), RunParameters> runParams = [];
    private readonly Dictionary<string, string> runNames = [];

    public void RegisterRunName(string runId, string name)
    {
        lock (gate)
        {
            runNames[runId] = name;
        }
    }

    public void ObserveTelemetry(TelemetryMessage telemetry, int windowSeconds)
    {
        lock (gate)
        {
            if (!seenTelemetry.Add(WindowMath.TelemetryDedupeKey(telemetry)))
            {
                return;
            }

            var key = WindowMath.Assign(telemetry, windowSeconds);
            var expected = GetOrCreateExpectedWindow(key);
            expected.Aggregate.Add(telemetry.Value);

            var score = TouchScore(telemetry.RunId, telemetry.TeamId);
            score.ObservedTelemetryCount++;
            score.LastUpdatedUtc = telemetry.PublishedAtUtc;
        }
    }

    public void ObserveControl(ControlMessage control, int windowSeconds)
    {
        lock (gate)
        {
            if (control.Event == Topics.PublisherStart
                && control.DeviceCount is > 0
                && control.MessageIntervalMs is > 0
                && control.RunWindowSeconds is > 0)
            {
                var parameters = new RunParameters(
                    control.DeviceCount.Value,
                    control.MessageIntervalMs.Value,
                    control.RunWindowSeconds.Value,
                    control.PublishedAtUtc,
                    windowSeconds);

                runParams[(control.RunId, control.TeamId)] = parameters;

                foreach (var expectation in RunMath.BuildWindowExpectations(
                             control.PublishedAtUtc,
                             control.RunWindowSeconds.Value,
                             control.MessageIntervalMs.Value,
                             windowSeconds))
                {
                    for (var deviceNumber = 1; deviceNumber <= control.DeviceCount.Value; deviceNumber++)
                    {
                        var key = new WindowKey(
                            control.RunId,
                            control.TeamId,
                            $"device-{deviceNumber:000}",
                            expectation.WindowStartUtc,
                            expectation.WindowEndUtc);

                        var expected = GetOrCreateExpectedWindow(key);
                        expected.TheoreticalCount = expectation.ExpectedCountPerDevice;
                    }
                }
            }

            var score = TouchScore(control.RunId, control.TeamId);
            score.LastUpdatedUtc = control.PublishedAtUtc;
        }
    }

    public void ObserveResult(AggregateResultMessage result, DateTimeOffset receivedAtUtc)
    {
        var key = new WindowKey(
            result.RunId,
            result.TeamId,
            result.DeviceId,
            result.WindowStartUtc,
            result.WindowEndUtc);

        lock (gate)
        {
            var score = TouchScore(result.RunId, result.TeamId);
            if (finalizedWindows.Contains(key) || resultCandidates.ContainsKey(key))
            {
                score.Invalid++;
                Invalid.Add(1, new KeyValuePair<string, object?>("team_id", result.TeamId));
                score.LastUpdatedUtc = receivedAtUtc;
                return;
            }

            resultCandidates.Add(key, new ResultCandidate(result, receivedAtUtc));
            score.LastUpdatedUtc = receivedAtUtc;
        }
    }

    public void AddInvalid(string runId, string teamId)
    {
        lock (gate)
        {
            var score = TouchScore(runId, teamId);
            score.Invalid++;
            Invalid.Add(1, new KeyValuePair<string, object?>("team_id", teamId));
            score.LastUpdatedUtc = DateTimeOffset.UtcNow;
        }
    }

    public void FinalizeDueWindows(int graceSeconds)
    {
        var now = DateTimeOffset.UtcNow;

        lock (gate)
        {
            foreach (var (key, expected) in expectedWindows)
            {
                if (finalizedWindows.Contains(key))
                {
                    continue;
                }

                if (now < key.WindowEndUtc.AddSeconds(graceSeconds).Add(FinalizationSlack))
                {
                    continue;
                }

                var score = TouchScore(key.RunId, key.TeamId);
                var observedCount = expected.Aggregate.Count;
                var theoreticalCount = expected.TheoreticalCount;

                if (observedCount != theoreticalCount)
                {
                    score.PublisherMismatchWindowCount++;
                    finalizedWindows.Add(key);
                    score.LastUpdatedUtc = now;
                    continue;
                }

                score.FullyObservedWindowCount++;

                if (resultCandidates.TryGetValue(key, out var candidate) && expected.Aggregate.Matches(candidate.Result))
                {
                    score.Correct++;
                    Correct.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
                    score.LatenciesMs.Add(Math.Max(0, (candidate.ReceivedAtUtc - key.WindowEndUtc).TotalMilliseconds));
                }
                else if (resultCandidates.ContainsKey(key))
                {
                    score.Invalid++;
                    Invalid.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
                }
                else
                {
                    score.Missing++;
                    Missing.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
                }

                finalizedWindows.Add(key);
                score.LastUpdatedUtc = now;
            }

            foreach (var (key, candidate) in resultCandidates)
            {
                if (expectedWindows.ContainsKey(key) || finalizedWindows.Contains(key))
                {
                    continue;
                }

                if (now - candidate.ReceivedAtUtc < TimeSpan.FromSeconds(graceSeconds))
                {
                    continue;
                }

                var score = TouchScore(key.RunId, key.TeamId);
                score.Invalid++;
                Invalid.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
                finalizedWindows.Add(key);
                score.LastUpdatedUtc = now;
            }
        }
    }

    public IReadOnlyList<ScoreSnapshot> GetScores()
    {
        lock (gate)
        {
            return scores
                .Select(entry => ToSnapshot(entry.Key.RunId, entry.Key.TeamId, entry.Value))
                .OrderByDescending(snapshot => snapshot.Score)
                .ThenBy(snapshot => snapshot.TeamId)
                .ToArray();
        }
    }

    public IReadOnlyList<ScoreSnapshot> GetScores(string runId)
    {
        lock (gate)
        {
            return scores
                .Where(entry => entry.Key.RunId == runId)
                .Select(entry => ToSnapshot(entry.Key.RunId, entry.Key.TeamId, entry.Value))
                .OrderByDescending(snapshot => snapshot.Score)
                .ThenBy(snapshot => snapshot.TeamId)
                .ToArray();
        }
    }

    private ExpectedWindow GetOrCreateExpectedWindow(WindowKey key)
    {
        if (!expectedWindows.TryGetValue(key, out var expected))
        {
            expected = new ExpectedWindow();
            expectedWindows.Add(key, expected);
        }

        return expected;
    }

    private ScoreState TouchScore(string runId, string teamId)
    {
        var key = (runId, teamId);
        if (!scores.TryGetValue(key, out var score))
        {
            score = new ScoreState();
            scores.Add(key, score);
        }

        return score;
    }

    private ScoreSnapshot ToSnapshot(string runId, string teamId, ScoreState state)
    {
        var latencyP95Ms = CalculateP95(state.LatenciesMs);
        runParams.TryGetValue((runId, teamId), out var parameters);

        var theoreticalTelemetryCount = parameters is null
            ? 0
            : RunMath.CalculateTheoreticalTelemetryCount(parameters.DeviceCount, parameters.RunWindowSeconds, parameters.MessageIntervalMs);
        var expectedWindowCount = parameters?.ExpectedWindowCount ?? 0;
        var publishAttainment = theoreticalTelemetryCount == 0
            ? 0
            : Math.Min(1d, (double)state.ObservedTelemetryCount / theoreticalTelemetryCount);
        var windowCorrectness = state.FullyObservedWindowCount == 0
            ? 0
            : (double)state.Correct / state.FullyObservedWindowCount;
        var windowInvalidRate = state.FullyObservedWindowCount == 0
            ? 0
            : (double)state.Invalid / state.FullyObservedWindowCount;
        var windowMissingRate = state.FullyObservedWindowCount == 0
            ? 0
            : (double)state.Missing / state.FullyObservedWindowCount;

        var intervalScore = parameters is null ? 0 : ScoreMath.CalculateIntervalScore(parameters.MessageIntervalMs);
        var deviceScore = parameters is null ? 0 : ScoreMath.CalculateDeviceScore(parameters.DeviceCount);
        var publishAttainmentScore = ScoreMath.CalculatePublishAttainmentScore(publishAttainment);
        var windowCorrectnessScore = ScoreMath.CalculateWindowCorrectnessScore(windowCorrectness);
        var latencyScore = ScoreMath.CalculateLatencyScore(latencyP95Ms);
        var score = intervalScore + deviceScore + publishAttainmentScore + windowCorrectnessScore + latencyScore;
        var name = runNames.GetValueOrDefault(runId, runId);

        return new ScoreSnapshot(
            runId,
            name,
            teamId,
            state.Correct,
            state.Invalid,
            state.Missing,
            latencyP95Ms,
            score,
            state.LastUpdatedUtc,
            "Running",
            parameters?.DeviceCount,
            parameters?.MessageIntervalMs,
            parameters?.RunWindowSeconds,
            theoreticalTelemetryCount,
            state.ObservedTelemetryCount,
            publishAttainment,
            expectedWindowCount,
            state.FullyObservedWindowCount,
            state.PublisherMismatchWindowCount,
            windowCorrectness,
            windowInvalidRate,
            windowMissingRate,
            intervalScore,
            deviceScore,
            publishAttainmentScore,
            windowCorrectnessScore,
            latencyScore);
    }

    private static double CalculateP95(IReadOnlyCollection<double> values)
    {
        if (values.Count == 0)
        {
            return 0;
        }

        var ordered = values.Order().ToArray();
        var index = (int)Math.Ceiling(ordered.Length * 0.95) - 1;
        return ordered[Math.Clamp(index, 0, ordered.Length - 1)];
    }

    private sealed class ExpectedWindow
    {
        public int TheoreticalCount { get; set; }

        public AggregateWindow Aggregate { get; } = new();
    }

    private sealed record ResultCandidate(AggregateResultMessage Result, DateTimeOffset ReceivedAtUtc);

    private sealed record RunParameters(
        int DeviceCount,
        int MessageIntervalMs,
        int RunWindowSeconds,
        DateTimeOffset RunStartedAtUtc,
        int WindowSeconds)
    {
        public int ExpectedWindowCount =>
            RunMath.BuildWindowExpectations(RunStartedAtUtc, RunWindowSeconds, MessageIntervalMs, WindowSeconds).Count * DeviceCount;
    }

    private sealed class ScoreState
    {
        public long ObservedTelemetryCount { get; set; }

        public int Correct { get; set; }

        public int Invalid { get; set; }

        public int Missing { get; set; }

        public int FullyObservedWindowCount { get; set; }

        public int PublisherMismatchWindowCount { get; set; }

        public List<double> LatenciesMs { get; } = [];

        public DateTimeOffset LastUpdatedUtc { get; set; } = DateTimeOffset.UtcNow;
    }
}
