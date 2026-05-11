using System.Net.Http.Json;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using TeamActivity.Shared.Contracts;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.AddHttpClient<JudgeClient>(client =>
{
    client.BaseAddress = new Uri("https+http://judge");
});

var app = builder.Build();

app.MapDefaultEndpoints();
app.MapGet("/", () => Results.Content(ScoreboardPage.IndexHtml, "text/html"));
app.MapGet("/api/runs", async (JudgeClient judge, CancellationToken cancellationToken) =>
{
    var runs = await judge.GetRuns(cancellationToken);
    return Results.Json(runs);
});
app.MapGet("/api/scores", async (JudgeClient judge, CancellationToken cancellationToken) =>
{
    var scores = await judge.GetScores(cancellationToken);
    return Results.Json(scores);
});
app.MapGet("/api/chaos", async (JudgeClient judge, CancellationToken cancellationToken) =>
{
    var chaos = await judge.GetChaos(cancellationToken);
    return Results.Json(chaos);
});

app.Run();

internal sealed class JudgeClient(HttpClient httpClient)
{
    private static readonly Histogram<double> RefreshDuration = TelemetryMeters.Scoreboard.CreateHistogram<double>("scoreboard_refresh_duration_ms");

    public async Task<IReadOnlyList<RunSnapshot>> GetRuns(CancellationToken cancellationToken)
    {
        return await Measure(() => httpClient.GetFromJsonAsync<IReadOnlyList<RunSnapshot>>("/api/runs", cancellationToken), cancellationToken) ?? [];
    }

    public async Task<IReadOnlyList<ScoreSnapshot>> GetScores(CancellationToken cancellationToken)
    {
        return await Measure(() => httpClient.GetFromJsonAsync<IReadOnlyList<ScoreSnapshot>>("/api/scores", cancellationToken), cancellationToken) ?? [];
    }

    public async Task<ChaosState?> GetChaos(CancellationToken cancellationToken)
    {
        return await Measure(() => httpClient.GetFromJsonAsync<ChaosState>("/api/chaos", cancellationToken), cancellationToken);
    }

    private static async Task<T?> Measure<T>(Func<Task<T?>> action, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            return await action();
        }
        finally
        {
            stopwatch.Stop();
            if (!cancellationToken.IsCancellationRequested)
            {
                RefreshDuration.Record(stopwatch.Elapsed.TotalMilliseconds);
            }
        }
    }
}

internal sealed record RunSnapshot(
    string RunId,
    IReadOnlyList<string> TeamIds,
    int MessageCount,
    int ValidMessageCount,
    int InvalidMessageCount,
    DateTimeOffset LastUpdatedUtc);

internal sealed record ScoreSnapshot(
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

internal sealed record ChaosState(
    bool Enabled,
    string? RunId,
    ActiveChaosEvent? ActiveEvent);

internal sealed record ActiveChaosEvent(
    string Id,
    string Type,
    string Description,
    DateTimeOffset StartedAtUtc);

internal static class ScoreboardPage
{
    public const string IndexHtml = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MQTT AI Battle Scoreboard</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; color: #1f2937; }
    table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
    th, td { border-bottom: 1px solid #e5e7eb; padding: .75rem; text-align: left; }
    th { background: #f9fafb; }
    .muted { color: #6b7280; }
    .bad { color: #b91c1c; font-weight: 600; }
    #chaos-banner { display: none; padding: .75rem 1.25rem; border-radius: .5rem; margin-bottom: 1.5rem; font-weight: 600; font-size: 1.1rem; }
    #chaos-banner.armed { display: block; background: #fef3c7; color: #92400e; border: 1px solid #fcd34d; }
    #chaos-banner.active { display: block; background: #fee2e2; color: #991b1b; border: 2px solid #f87171; animation: pulse 1.5s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .7; } }
  </style>
</head>
<body>
  <h1>MQTT AI Battle</h1>
  <div id="chaos-banner"></div>
  <p class="muted">Live scoreboard — aggregate results scored in real time by the Judge.</p>
  <h2>Leaderboard</h2>
  <table>
    <thead>
      <tr>
        <th>Run</th>
        <th>Team</th>
        <th>Score</th>
        <th>Correct</th>
        <th>Invalid</th>
        <th>Missing</th>
        <th>Latency p95 ms</th>
        <th>Devices</th>
        <th>Interval ms</th>
      </tr>
    </thead>
    <tbody id="scores">
      <tr><td colspan="9" class="muted">Waiting for scores...</td></tr>
    </tbody>
  </table>
  <h2>Observed messages</h2>
  <table>
    <thead>
      <tr>
        <th>Run</th>
        <th>Teams</th>
        <th>Messages</th>
        <th>Valid</th>
        <th>Invalid</th>
        <th>Last update</th>
      </tr>
    </thead>
    <tbody id="runs">
      <tr><td colspan="6" class="muted">Waiting for Judge data...</td></tr>
    </tbody>
  </table>
  <script>
    function esc(val) {
      const el = document.createElement('span');
      el.textContent = val ?? '';
      return el.textContent;
    }

    async function refresh() {
      const [runsResult, scoresResult, chaosResult] = await Promise.allSettled([
        fetch('/api/runs').then(r => r.json()),
        fetch('/api/scores').then(r => r.json()),
        fetch('/api/chaos').then(r => r.json())
      ]);

      if (chaosResult.status === 'fulfilled') {
        updateChaosBanner(chaosResult.value);
      }

      if (scoresResult.status === 'fulfilled') {
        const scores = scoresResult.value;
        const scoresBody = document.getElementById('scores');
        if (!scores.length) {
          scoresBody.innerHTML = '<tr><td colspan="9" class="muted">No scores yet.</td></tr>';
        } else {
          scoresBody.innerHTML = scores.map(score => `
            <tr>
              <td>${esc(score.runId)}</td>
              <td>${esc(score.teamId)}</td>
              <td>${score.score.toFixed(2)}</td>
              <td>${score.correct}</td>
              <td class="${score.invalid ? 'bad' : ''}">${score.invalid}</td>
              <td class="${score.missing ? 'bad' : ''}">${score.missing}</td>
              <td>${score.latencyP95Ms.toFixed(0)}</td>
              <td>${score.deviceCount ?? '—'}</td>
              <td>${score.messageIntervalMs ?? '—'}</td>
            </tr>
          `).join('');
        }
      }

      if (runsResult.status === 'fulfilled') {
        const runs = runsResult.value;
        const body = document.getElementById('runs');
        if (!runs.length) {
          body.innerHTML = '<tr><td colspan="6" class="muted">No runs observed yet.</td></tr>';
        } else {
          body.innerHTML = runs.map(run => `
            <tr>
              <td>${esc(run.runId)}</td>
              <td>${run.teamIds.map(esc).join(', ')}</td>
              <td>${run.messageCount}</td>
              <td>${run.validMessageCount}</td>
              <td class="${run.invalidMessageCount ? 'bad' : ''}">${run.invalidMessageCount}</td>
              <td>${new Date(run.lastUpdatedUtc).toLocaleString()}</td>
            </tr>
          `).join('');
        }
      }
    }

    function updateChaosBanner(chaos) {
      const banner = document.getElementById('chaos-banner');
      banner.className = '';
      banner.textContent = '';
      if (!chaos || !chaos.enabled) return;
      if (chaos.activeEvent) {
        banner.className = 'active';
        banner.textContent = '\uD83D\uDD25 CHAOS: ' + chaos.activeEvent.type.toUpperCase().replace(/-/g, ' ');
        const desc = chaos.activeEvent.description;
        if (desc) {
          const small = document.createElement('span');
          small.style.fontWeight = 'normal';
          small.style.marginLeft = '.75rem';
          small.textContent = desc;
          banner.appendChild(small);
        }
      } else {
        banner.className = 'armed';
        banner.textContent = '\u26A0\uFE0F Chaos Mode — disruptions may occur at any time';
      }
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
""";
}
