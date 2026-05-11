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
    private readonly HashSet<WindowKey> scoredWindows = [];
    private readonly HashSet<WindowKey> missingWindows = [];
    private readonly Dictionary<(string RunId, string TeamId), ScoreState> scores = [];
    private readonly Dictionary<(string RunId, string TeamId), bool> completedRuns = [];
    private readonly Dictionary<(string RunId, string TeamId), (int? DeviceCount, int? MessageIntervalMs)> runParams = [];

    public void ObserveTelemetry(TelemetryMessage telemetry, int windowSeconds)
    {
        lock (gate)
        {
            if (!seenTelemetry.Add(WindowMath.TelemetryDedupeKey(telemetry)))
            {
                return;
            }

            var key = WindowMath.Assign(telemetry, windowSeconds);
            if (!expectedWindows.TryGetValue(key, out var expected))
            {
                expected = new ExpectedWindow(key);
                expectedWindows.Add(key, expected);
            }

            expected.Aggregate.Add(telemetry.Value);
            TouchScore(telemetry.RunId, telemetry.TeamId);
        }
    }

    public void ObserveControl(ControlMessage control)
    {
        lock (gate)
        {
            if (control.Event == Topics.PublisherComplete)
            {
                completedRuns[(control.RunId, control.TeamId)] = true;
            }

            if (control.Event == Topics.PublisherStart && (control.DeviceCount.HasValue || control.MessageIntervalMs.HasValue))
            {
                runParams[(control.RunId, control.TeamId)] = (control.DeviceCount, control.MessageIntervalMs);
            }

            TouchScore(control.RunId, control.TeamId);
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
            if (scoredWindows.Contains(key) || missingWindows.Contains(key) || resultCandidates.ContainsKey(key))
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
                if (scoredWindows.Contains(key) || missingWindows.Contains(key))
                {
                    continue;
                }

                var runComplete = completedRuns.GetValueOrDefault((key.RunId, key.TeamId));
                if (!runComplete || now < key.WindowEndUtc.AddSeconds(graceSeconds).Add(FinalizationSlack))
                {
                    continue;
                }

                var score = TouchScore(key.RunId, key.TeamId);
                if (resultCandidates.TryGetValue(key, out var candidate) && expected.Aggregate.Matches(candidate.Result))
                {
                    score.Correct++;
                    Correct.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
                    score.LatenciesMs.Add(Math.Max(0, (candidate.ReceivedAtUtc - key.WindowEndUtc).TotalMilliseconds));
                    scoredWindows.Add(key);
                }
                else
                {
                    if (resultCandidates.ContainsKey(key))
                    {
                        score.Invalid++;
                        Invalid.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
                    }

                    score.Missing++;
                    Missing.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
                    missingWindows.Add(key);
                }

                score.LastUpdatedUtc = now;
            }

            foreach (var (key, candidate) in resultCandidates)
            {
                if (expectedWindows.ContainsKey(key) || scoredWindows.Contains(key) || missingWindows.Contains(key))
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
                missingWindows.Add(key);
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
        var score = state.Correct - 5 * state.Invalid - 3 * state.Missing - 0.05 * latencyP95Ms;
        runParams.TryGetValue((runId, teamId), out var rp);
        return new ScoreSnapshot(
            runId,
            teamId,
            state.Correct,
            state.Invalid,
            state.Missing,
            latencyP95Ms,
            score,
            state.LastUpdatedUtc,
            "Running",
            rp.DeviceCount,
            rp.MessageIntervalMs);
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

    private sealed class ExpectedWindow(WindowKey key)
    {
        public WindowKey Key { get; } = key;

        public AggregateWindow Aggregate { get; } = new();
    }

    private sealed record ResultCandidate(AggregateResultMessage Result, DateTimeOffset ReceivedAtUtc);

    private sealed class ScoreState
    {
        public int Correct { get; set; }

        public int Invalid { get; set; }

        public int Missing { get; set; }

        public List<double> LatenciesMs { get; } = [];

        public DateTimeOffset LastUpdatedUtc { get; set; } = DateTimeOffset.UtcNow;
    }
}
