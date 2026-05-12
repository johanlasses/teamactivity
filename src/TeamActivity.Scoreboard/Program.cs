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
app.MapGet("/api/run/status", async (JudgeClient judge, CancellationToken cancellationToken) =>
{
    var status = await judge.GetRunStatus(cancellationToken);
    return Results.Json(status);
});
app.MapPost("/api/run/start", async (RunStartRequest req, JudgeClient judge, CancellationToken cancellationToken) =>
{
    return await judge.StartRun(req, cancellationToken);
});
app.MapPost("/api/run/stop", async (JudgeClient judge, CancellationToken cancellationToken) =>
{
    return await judge.StopRun(cancellationToken);
});

app.MapPost("/api/chaos/event/start", async (HttpRequest req, ChaosEventStartRequest body, JudgeClient judge, CancellationToken cancellationToken) =>
{
    var organizerKey = req.Headers.TryGetValue("X-Organizer-Key", out var key) ? key.ToString() : null;
    return await judge.StartChaosEvent(body.Type, body.Description, organizerKey, cancellationToken);
});

app.MapPost("/api/chaos/event/end", async (HttpRequest req, JudgeClient judge, CancellationToken cancellationToken) =>
{
    var organizerKey = req.Headers.TryGetValue("X-Organizer-Key", out var key) ? key.ToString() : null;
    return await judge.EndChaosEvent(organizerKey, cancellationToken);
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

    public async Task<RunStatusSnapshot?> GetRunStatus(CancellationToken cancellationToken)
    {
        return await Measure(() => httpClient.GetFromJsonAsync<RunStatusSnapshot>("/api/run/status", cancellationToken), cancellationToken);
    }

    public async Task<IResult> StartRun(RunStartRequest req, CancellationToken cancellationToken)
    {
        try
        {
            var response = await httpClient.PostAsJsonAsync("/api/run/start", req, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            return response.IsSuccessStatusCode
                ? Results.Content(body, "application/json", statusCode: (int)response.StatusCode)
                : Results.Problem(body, statusCode: (int)response.StatusCode);
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to reach Judge: {ex.Message}", statusCode: 502);
        }
    }

    public async Task<IResult> StopRun(CancellationToken cancellationToken)
    {
        try
        {
            var response = await httpClient.PostAsync("/api/run/stop", null, cancellationToken);
            return response.IsSuccessStatusCode
                ? Results.Ok()
                : Results.Problem(await response.Content.ReadAsStringAsync(cancellationToken), statusCode: (int)response.StatusCode);
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to reach Judge: {ex.Message}", statusCode: 502);
        }
    }

    public async Task<IResult> StartChaosEvent(string type, string? description, string? organizerKey, CancellationToken cancellationToken)
    {
        try
        {
            var request = new HttpRequestMessage(HttpMethod.Post, "/api/chaos/event/start")
            {
                Content = JsonContent.Create(new { type, description })
            };
            if (!string.IsNullOrEmpty(organizerKey))
                request.Headers.Add("X-Organizer-Key", organizerKey);
            var response = await httpClient.SendAsync(request, cancellationToken);
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            return response.IsSuccessStatusCode
                ? Results.Content(body, "application/json", statusCode: (int)response.StatusCode)
                : Results.Problem(body, statusCode: (int)response.StatusCode);
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to reach Judge: {ex.Message}", statusCode: 502);
        }
    }

    public async Task<IResult> EndChaosEvent(string? organizerKey, CancellationToken cancellationToken)
    {
        try
        {
            var request = new HttpRequestMessage(HttpMethod.Post, "/api/chaos/event/end");
            if (!string.IsNullOrEmpty(organizerKey))
                request.Headers.Add("X-Organizer-Key", organizerKey);
            var response = await httpClient.SendAsync(request, cancellationToken);
            return response.IsSuccessStatusCode
                ? Results.Ok()
                : Results.Problem(await response.Content.ReadAsStringAsync(cancellationToken), statusCode: (int)response.StatusCode);
        }
        catch (Exception ex)
        {
            return Results.Problem($"Failed to reach Judge: {ex.Message}", statusCode: 502);
        }
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
    string Name,
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

internal sealed record RunStartRequest(int DeviceCount, int IntervalMs, int RunWindowSeconds, bool ChaosEnabled);
internal sealed record RunTriggerConfig(string RunId, string Name, int DeviceCount, int IntervalMs, int RunWindowSeconds, bool ChaosEnabled);
internal sealed record RunStatusSnapshot(string Status, RunTriggerConfig? Config);
internal sealed record ChaosEventStartRequest(string Type, string? Description);

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
    #control-panel { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: .75rem; padding: 1.5rem; margin-bottom: 2rem; }
    #control-panel h2 { margin-top: 0; }
    .control-row { display: flex; flex-wrap: wrap; gap: 1rem; align-items: flex-end; margin-bottom: 1rem; }
    .control-field label { display: block; font-size: .85rem; color: #6b7280; margin-bottom: .25rem; }
    .control-field input[type=number] { width: 90px; padding: .4rem .6rem; border: 1px solid #d1d5db; border-radius: .375rem; font-size: 1rem; }
    .control-field input[type=number]:disabled, #start-btn:disabled, #stop-btn:disabled { opacity: .5; cursor: not-allowed; }
    .chaos-field { display: flex; align-items: center; gap: .5rem; padding-bottom: .35rem; }
    #start-btn { padding: .5rem 1.25rem; background: #2563eb; color: white; border: none; border-radius: .375rem; font-size: 1rem; font-weight: 600; cursor: pointer; }
    #start-btn:hover:not(:disabled) { background: #1d4ed8; }
    #stop-btn { padding: .5rem 1.25rem; background: #dc2626; color: white; border: none; border-radius: .375rem; font-size: 1rem; font-weight: 600; cursor: pointer; display: none; }
    #stop-btn:hover:not(:disabled) { background: #b91c1c; }
    #run-status-badge { display: inline-block; padding: .25rem .75rem; border-radius: 9999px; font-size: .85rem; font-weight: 600; }
    #run-status-badge.idle { background: #d1fae5; color: #065f46; }
    #run-status-badge.pending { background: #fef3c7; color: #92400e; }
    #run-status-badge.running { background: #dbeafe; color: #1e40af; }
    #start-feedback { margin-top: .75rem; font-size: .9rem; }
    #start-feedback.error { color: #b91c1c; }
    #start-feedback.success { color: #065f46; }
    #organizer-panel { background: #fdf4ff; border: 1px solid #e9d5ff; border-radius: .75rem; padding: 1.25rem; margin-bottom: 2rem; display: none; }
    #organizer-panel h3 { margin: 0 0 .75rem; color: #6b21a8; font-size: 1rem; }
    .organizer-row { display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; margin-bottom: .75rem; }
    .chaos-btn { padding: .4rem .85rem; background: #7c3aed; color: white; border: none; border-radius: .375rem; font-size: .875rem; font-weight: 600; cursor: pointer; }
    .chaos-btn:hover { background: #6d28d9; }
    .chaos-btn:disabled { opacity: .5; cursor: not-allowed; }
    #end-event-btn { padding: .4rem .85rem; background: #374151; color: white; border: none; border-radius: .375rem; font-size: .875rem; font-weight: 600; cursor: pointer; }
    #end-event-btn:hover { background: #1f2937; }
    #organizer-key-input { padding: .35rem .6rem; border: 1px solid #d1d5db; border-radius: .375rem; font-size: .875rem; width: 200px; }
    #organizer-toggle { padding: .3rem .75rem; background: #f3f4f6; border: 1px solid #d1d5db; border-radius: .375rem; font-size: .85rem; cursor: pointer; float: right; margin-top: -2.5rem; }
    #chaos-feedback { font-size: .85rem; margin-top: .5rem; }
    #chaos-feedback.error { color: #b91c1c; }
    #chaos-feedback.success { color: #065f46; }
  </style>
</head>
<body>
  <h1>MQTT AI Battle</h1>
  <div id="chaos-banner"></div>

  <div id="control-panel">
    <h2>Start a Run <button id="organizer-toggle" onclick="toggleOrganizerPanel()" title="Organizer controls">🎛 Organizer</button></h2>
    <div class="control-row">
      <div class="control-field">
        <label for="device-count">Device Count</label>
        <input type="number" id="device-count" value="3" min="1" max="100">
      </div>
      <div class="control-field">
        <label for="message-interval">Message Interval (ms)</label>
        <input type="number" id="message-interval" value="250" min="1">
      </div>
      <div class="control-field">
        <label for="run-window">Run Window (seconds)</label>
        <input type="number" id="run-window" value="120" min="1">
      </div>
      <div class="control-field chaos-field">
        <input type="checkbox" id="chaos-mode">
        <label for="chaos-mode" style="margin:0">Enable Chaos Mode</label>
      </div>
      <div class="control-field">
        <button id="start-btn" onclick="startRun()">▶ Start Run</button>
        <button id="stop-btn" onclick="stopRun()" style="margin-left:.5rem">⏹ Stop Run</button>
      </div>
      <div class="control-field" style="padding-bottom:.35rem">
        Status: <span id="run-status-badge" class="idle">Idle</span>
      </div>
    </div>
    <div id="start-feedback"></div>
  </div>

  <div id="organizer-panel">
    <h3>🎛 Organizer — Chaos Controls</h3>
    <div class="organizer-row">
      <label style="font-size:.85rem;color:#6b7280;margin-right:.25rem">Key (if set):</label>
      <input type="password" id="organizer-key-input" placeholder="X-Organizer-Key (optional)" oninput="saveOrganizerKey(this.value)">
    </div>
    <div class="organizer-row">
      <button class="chaos-btn" onclick="fireChaosEvent('processor-restart','Processor service restarted')">🔄 Processor Restart</button>
      <button class="chaos-btn" onclick="fireChaosEvent('message-burst','Publisher briefly sending at a much faster rate')">💥 Message Burst</button>
      <button class="chaos-btn" onclick="fireChaosEvent('message-gap','Publisher paused for several seconds')">⏸ Message Gap</button>
      <button class="chaos-btn" onclick="fireChaosEvent('device-dropout','One device stopped sending')">📵 Device Dropout</button>
      <button class="chaos-btn" onclick="fireChaosEvent('high-latency','Artificial delay injected between publisher and broker')">🐢 High Latency</button>
      <button id="end-event-btn" onclick="endChaosEvent()">✅ End Event</button>
    </div>
    <div id="chaos-feedback"></div>
  </div>

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

    function setControlsEnabled(enabled) {
      ['device-count', 'message-interval', 'run-window', 'chaos-mode', 'start-btn'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.disabled = !enabled;
      });
      const stopBtn = document.getElementById('stop-btn');
      if (stopBtn) stopBtn.style.display = enabled ? 'none' : 'inline-block';
    }

    function updateRunStatusBadge(status) {
      const badge = document.getElementById('run-status-badge');
      const normalized = (status || 'idle').toLowerCase();
      badge.className = 'run-status-badge ' + normalized;
      badge.textContent = normalized.charAt(0).toUpperCase() + normalized.slice(1);
      setControlsEnabled(normalized === 'idle');
    }

    async function stopRun() {
      const stopBtn = document.getElementById('stop-btn');
      const feedback = document.getElementById('start-feedback');
      if (stopBtn) stopBtn.disabled = true;
      feedback.className = '';
      feedback.textContent = 'Stopping run…';
      try {
        const res = await fetch('/api/run/stop', { method: 'POST' });
        if (res.ok) {
          feedback.className = 'success';
          feedback.textContent = 'Run stopped.';
        } else {
          feedback.className = 'error';
          feedback.textContent = 'Stop failed: ' + res.status;
          if (stopBtn) stopBtn.disabled = false;
        }
      } catch (err) {
        feedback.className = 'error';
        feedback.textContent = 'Network error: ' + err.message;
        if (stopBtn) stopBtn.disabled = false;
      }
    }

    async function startRun() {
      const deviceCount = parseInt(document.getElementById('device-count').value, 10);
      const intervalMs = parseInt(document.getElementById('message-interval').value, 10);
      const runWindowSeconds = parseInt(document.getElementById('run-window').value, 10);
      const chaosEnabled = document.getElementById('chaos-mode').checked;
      const feedback = document.getElementById('start-feedback');

      if (!deviceCount || deviceCount < 1) {
        feedback.className = 'error';
        feedback.textContent = 'Device Count must be at least 1.';
        return;
      }
      if (!intervalMs || intervalMs < 1) {
        feedback.className = 'error';
        feedback.textContent = 'Message Interval must be at least 1 ms.';
        return;
      }
      if (!runWindowSeconds || runWindowSeconds < 1) {
        feedback.className = 'error';
        feedback.textContent = 'Run Window must be at least 1 second.';
        return;
      }

      setControlsEnabled(false);
      feedback.className = '';
      feedback.textContent = 'Starting run…';

      try {
        const res = await fetch('/api/run/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ deviceCount, intervalMs, runWindowSeconds, chaosEnabled })
        });

        if (res.ok) {
          const data = await res.json();
          feedback.className = 'success';
          feedback.textContent = '';
          feedback.appendChild(document.createTextNode('Run started: '));
          const strong = document.createElement('strong');
          strong.textContent = data.name || data.runId;
          feedback.appendChild(strong);
        } else {
          const text = await res.text();
          feedback.className = 'error';
          feedback.textContent = 'Failed to start run: ' + res.status + ' — ' + text;
          setControlsEnabled(true);
        }
      } catch (err) {
        feedback.className = 'error';
        feedback.textContent = 'Network error: ' + err.message;
        setControlsEnabled(true);
      }
    }

    async function refresh() {
      const [runsResult, scoresResult, chaosResult, statusResult] = await Promise.allSettled([
        fetch('/api/runs').then(r => r.json()),
        fetch('/api/scores').then(r => r.json()),
        fetch('/api/chaos').then(r => r.json()),
        fetch('/api/run/status').then(r => r.json())
      ]);

      if (chaosResult.status === 'fulfilled') {
        updateChaosBanner(chaosResult.value);
      }

      if (statusResult.status === 'fulfilled' && statusResult.value) {
        updateRunStatusBadge(statusResult.value.status);
      }

      if (scoresResult.status === 'fulfilled') {
        const scores = scoresResult.value;
        const scoresBody = document.getElementById('scores');
        if (!scores.length) {
          scoresBody.innerHTML = '<tr><td colspan="9" class="muted">No scores yet.</td></tr>';
        } else {
          scoresBody.innerHTML = scores.map(score => `
            <tr>
              <td title="${esc(score.runId)}">${esc(score.name || score.runId)}</td>
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
      if (!chaos || !chaos.enabled) {
        document.getElementById('organizer-panel').style.display = 'none';
        return;
      }
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

    let organizerPanelVisible = false;
    function toggleOrganizerPanel() {
      organizerPanelVisible = !organizerPanelVisible;
      const panel = document.getElementById('organizer-panel');
      panel.style.display = organizerPanelVisible ? 'block' : 'none';
      if (organizerPanelVisible) {
        const saved = sessionStorage.getItem('organizerKey') || '';
        document.getElementById('organizer-key-input').value = saved;
      }
    }

    function saveOrganizerKey(val) {
      sessionStorage.setItem('organizerKey', val);
    }

    async function fireChaosEvent(type, description) {
      const key = sessionStorage.getItem('organizerKey') || '';
      const feedback = document.getElementById('chaos-feedback');
      feedback.className = '';
      feedback.textContent = 'Firing ' + type + '…';
      try {
        const headers = { 'Content-Type': 'application/json' };
        if (key) headers['X-Organizer-Key'] = key;
        const res = await fetch('/api/chaos/event/start', {
          method: 'POST',
          headers,
          body: JSON.stringify({ type, description })
        });
        if (res.ok) {
          feedback.className = 'success';
          feedback.textContent = '🔥 ' + type + ' started.';
        } else {
          feedback.className = 'error';
          feedback.textContent = 'Failed: ' + res.status + (key ? '' : ' (organizer key required?)');
        }
      } catch (err) {
        feedback.className = 'error';
        feedback.textContent = 'Network error: ' + err.message;
      }
    }

    async function endChaosEvent() {
      const key = sessionStorage.getItem('organizerKey') || '';
      const feedback = document.getElementById('chaos-feedback');
      feedback.className = '';
      feedback.textContent = 'Ending event…';
      try {
        const headers = {};
        if (key) headers['X-Organizer-Key'] = key;
        const res = await fetch('/api/chaos/event/end', { method: 'POST', headers });
        if (res.ok) {
          feedback.className = 'success';
          feedback.textContent = '✅ Event ended.';
        } else {
          feedback.className = 'error';
          feedback.textContent = 'Failed: ' + res.status;
        }
      } catch (err) {
        feedback.className = 'error';
        feedback.textContent = 'Network error: ' + err.message;
      }
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
""";
}
