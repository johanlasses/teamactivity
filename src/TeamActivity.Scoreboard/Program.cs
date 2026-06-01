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

app.MapGet("/api/chaos/schedule", async (JudgeClient judge, CancellationToken cancellationToken) =>
{
    var schedule = await judge.GetChaosSchedule(cancellationToken);
    return Results.Json(schedule);
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

    public async Task<object?> GetChaosSchedule(CancellationToken cancellationToken)
    {
        return await Measure(() => httpClient.GetFromJsonAsync<object>("/api/chaos/schedule", cancellationToken), cancellationToken);
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
    string Name,
    IReadOnlyList<string> TeamIds,
    long MessageCount,
    long ValidMessageCount,
    long InvalidMessageCount,
    DateTimeOffset LastUpdatedUtc,
    long? TheoreticalTelemetryCount,
    long TelemetryMessageCount,
    double PublishAttainment,
    long ResultMessageCount,
    long ControlMessageCount);

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
    int? MessageIntervalMs,
    int? RunWindowSeconds,
    long TheoreticalTelemetryCount,
    long ObservedTelemetryCount,
    double PublishAttainment,
    int ExpectedWindowCount,
    int FullyObservedWindowCount,
    int PublisherMismatchWindowCount,
    double WindowCorrectness,
    double WindowInvalidRate,
    double WindowMissingRate,
    double IntervalScore,
    double DeviceScore,
    double PublishAttainmentScore,
    double WindowCorrectnessScore,
    double LatencyScore);

internal sealed record ChaosState(
    bool Enabled,
    string? RunId,
    ActiveChaosEvent? ActiveEvent);

internal sealed record ActiveChaosEvent(
    string Id,
    string Type,
    string Description,
    DateTimeOffset StartedAtUtc);

internal sealed record RunStartRequest(int DeviceCount, int IntervalMs, int RunWindowSeconds, bool ChaosEnabled, bool ChaosScheduleEnabled);
internal sealed record RunTriggerConfig(string RunId, string Name, int DeviceCount, int IntervalMs, int RunWindowSeconds, bool ChaosEnabled, bool ChaosScheduleEnabled);
internal sealed record RunStatusSnapshot(string Status, RunTriggerConfig? Config, DateTimeOffset? StartedAtUtc);
internal sealed record ChaosEventStartRequest(string Type, string? Description);

internal static class ScoreboardPage
{
    public const string IndexHtml = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MQTT AI Battle — Scoreboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --green:      #76b900;
      --green-dark: #5a8d00;
      --ink:        #000000;
      --canvas:     #ffffff;
      --surface:    #000000;
      --soft:       #f7f7f7;
      --elevated:   #1a1a1a;
      --hairline:   #cccccc;
      --hairline-s: #5e5e5e;
      --body:       #1a1a1a;
      --muted:      #757575;
      --on-dark:    #ffffff;
      --on-dark-m:  rgba(255,255,255,0.7);
      --red:        #e52020;
      --red-dark:   #b91c1c;
      --warn:       #df6500;
      --warn-bg:    #fef3c7;
      --radius:     2px;
    }

    body {
      font-family: 'Inter', Arial, Helvetica, sans-serif;
      font-size: 16px;
      line-height: 1.5;
      color: var(--body);
      background: var(--canvas);
    }

    /* ── Header ─────────────────────────────────────────────── */
    header {
      background: var(--surface);
      padding: 0 48px;
      height: 64px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      position: sticky;
      top: 0;
      z-index: 100;
    }
    .header-brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .corner-sq {
      width: 12px;
      height: 12px;
      background: var(--green);
      flex-shrink: 0;
    }
    .header-title {
      font-size: 18px;
      font-weight: 700;
      color: var(--on-dark);
      letter-spacing: 0;
    }
    .header-right {
      display: flex;
      align-items: center;
      gap: 16px;
    }

    /* ── Chaos banner ────────────────────────────────────────── */
    #chaos-banner {
      display: none;
      padding: 12px 48px;
      font-weight: 700;
      font-size: 15px;
    }
    #chaos-banner.armed {
      display: block;
      background: var(--warn-bg);
      color: #92400e;
      border-bottom: 2px solid #fcd34d;
    }
    #chaos-banner.active {
      display: block;
      background: #fee2e2;
      color: var(--red-dark);
      border-bottom: 2px solid var(--red);
      animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.75} }

    /* ── Main layout ─────────────────────────────────────────── */
    main { max-width: 1280px; margin: 0 auto; padding: 32px 48px 64px; }

    /* ── Card ────────────────────────────────────────────────── */
    .card {
      background: var(--canvas);
      border: 1px solid var(--hairline);
      border-radius: var(--radius);
      padding: 24px;
      margin-bottom: 24px;
      position: relative;
    }
    .card-corner { position: absolute; top: 0; left: 0; width: 12px; height: 12px; background: var(--green); }
    .card-title {
      font-size: 17px;
      font-weight: 700;
      color: var(--ink);
      margin-bottom: 16px;
      padding-left: 4px;
    }

    /* ── Buttons ─────────────────────────────────────────────── */
    .btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 10px 22px;
      font-family: inherit;
      font-size: 16px;
      font-weight: 700;
      border: none;
      border-radius: var(--radius);
      cursor: pointer;
      transition: background .15s;
    }
    .btn:disabled { opacity: .45; cursor: not-allowed; }
    .btn-primary { background: var(--green); color: var(--ink); }
    .btn-primary:hover:not(:disabled) { background: var(--green-dark); }
    .btn-danger  { background: var(--red);   color: var(--on-dark); }
    .btn-danger:hover:not(:disabled)  { background: var(--red-dark); }
    .btn-ghost   { background: var(--soft);  color: var(--ink); border: 1px solid var(--hairline); }
    .btn-ghost:hover:not(:disabled)   { background: var(--hairline); }
    .btn-sm { padding: 7px 14px; font-size: 14px; }

    /* ── Form controls ───────────────────────────────────────── */
    .control-row {
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      align-items: flex-end;
    }
    .field label {
      display: block;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .5px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .field input[type=number],
    .field input[type=password] {
      width: 110px;
      padding: 10px 12px;
      border: 1px solid var(--hairline);
      border-radius: var(--radius);
      font-family: inherit;
      font-size: 15px;
      color: var(--ink);
      background: var(--canvas);
    }
    .field input[type=number]:disabled { opacity: .45; }
    .field input[type=password] { width: 200px; }
    .check-field {
      display: flex;
      align-items: center;
      gap: 8px;
      padding-bottom: 2px;
    }
    .check-field input[type=checkbox] { width: 16px; height: 16px; accent-color: var(--green); cursor: pointer; }
    .check-field label { font-size: 14px; font-weight: 700; color: var(--ink); cursor: pointer; margin: 0; }

    /* ── Status badge ────────────────────────────────────────── */
    #run-status-badge {
      display: inline-block;
      padding: 4px 12px;
      border-radius: 9999px;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .5px;
    }
    #run-status-badge.idle    { background: #d1fae5; color: #065f46; }
    #run-status-badge.pending { background: var(--warn-bg); color: #92400e; }
    #run-status-badge.running { background: #dbeafe; color: #1e40af; }

    /* ── Run timer ───────────────────────────────────────────── */
    #run-timer { display: none; margin-top: 16px; }
    #run-timer.visible { display: block; }
    .timer-label {
      display: flex;
      justify-content: space-between;
      font-size: 13px;
      font-weight: 700;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .timer-elapsed { color: var(--green); }
    .progress-track {
      width: 100%;
      height: 6px;
      background: var(--soft);
      border-radius: var(--radius);
      overflow: hidden;
    }
    .progress-fill {
      height: 100%;
      background: var(--green);
      border-radius: var(--radius);
      transition: width .8s linear;
    }

    /* ── Feedback messages ───────────────────────────────────── */
    #start-feedback, #chaos-feedback {
      margin-top: 12px;
      font-size: 14px;
      min-height: 20px;
    }
    #start-feedback.error, #chaos-feedback.error { color: var(--red); }
    #start-feedback.success, #chaos-feedback.success { color: var(--green-dark); }

    /* ── Organizer panel ─────────────────────────────────────── */
    #organizer-panel {
      display: none;
      background: #fdf4ff;
      border: 1px solid #e9d5ff;
      border-radius: var(--radius);
      padding: 20px;
      margin-bottom: 24px;
    }
    .organizer-title {
      font-size: 14px;
      font-weight: 700;
      color: #6b21a8;
      text-transform: uppercase;
      letter-spacing: .5px;
      margin-bottom: 12px;
    }
    .organizer-row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 12px; }
    .chaos-btn {
      padding: 8px 16px;
      background: #7c3aed;
      color: white;
      border: none;
      border-radius: var(--radius);
      font-family: inherit;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
      transition: background .15s;
    }
    .chaos-btn:hover { background: #6d28d9; }
    .chaos-btn:disabled { opacity: .5; cursor: not-allowed; }
    #end-event-btn {
      padding: 8px 16px;
      background: var(--elevated);
      color: var(--on-dark);
      border: none;
      border-radius: var(--radius);
      font-family: inherit;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
    }
    #end-event-btn:hover { background: #374151; }

    /* ── Schedule preview ────────────────────────────────────── */
    #schedule-preview {
      display: none;
      margin-top: 12px;
      border: 1px solid var(--hairline);
      border-radius: var(--radius);
      overflow: hidden;
    }
    #schedule-preview.visible { display: block; }
    #schedule-preview table { margin: 0; }

    /* ── Tables ──────────────────────────────────────────────── */
    table {
      border-collapse: collapse;
      width: 100%;
      font-size: 14px;
    }
    thead { background: var(--soft); }
    th {
      padding: 10px 12px;
      text-align: left;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .5px;
      color: var(--muted);
      border-bottom: 1px solid var(--hairline);
      white-space: nowrap;
    }
    td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--hairline);
      color: var(--body);
    }
    tbody tr:last-child td { border-bottom: none; }
    tbody tr:hover { background: var(--soft); }
    .td-muted { color: var(--muted); font-style: italic; }
    .td-bad { color: var(--red); font-weight: 700; }
    .td-score { font-weight: 700; color: var(--ink); }
    .td-rank { font-weight: 700; color: var(--muted); font-size: 13px; }
    .td-name { font-weight: 700; color: var(--ink); }
    .medal-1 { color: #c9a227; }
    .medal-2 { color: #8c8c8c; }
    .medal-3 { color: #a0522d; }

    /* ── Section heading ─────────────────────────────────────── */
    .section-eyebrow {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: var(--green);
      margin-bottom: 4px;
    }
    .section-heading {
      font-size: 22px;
      font-weight: 700;
      color: var(--ink);
      margin-bottom: 4px;
    }
    .section-sub {
      font-size: 14px;
      color: var(--muted);
      margin-bottom: 20px;
    }

    /* ── Score breakdown bars ────────────────────────────────── */
    .score-breakdown {
      padding: 16px 12px;
      background: var(--soft);
      display: grid;
      grid-template-columns: 160px 1fr 52px;
      row-gap: 10px;
      column-gap: 12px;
      align-items: center;
    }
    .bar-label {
      font-size: 12px;
      font-weight: 700;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: .4px;
      white-space: nowrap;
    }
    .bar-track {
      height: 14px;
      background: #e5e7eb;
      border-radius: 2px;
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      border-radius: 2px;
      transition: width .6s ease;
    }
    .bar-value {
      font-size: 12px;
      font-weight: 700;
      color: var(--body);
      text-align: right;
      white-space: nowrap;
    }
    .expand-row { cursor: pointer; user-select: none; }
    .expand-row:hover td { background: #f0f0f0; }
    .expand-indicator {
      display: inline-block;
      font-size: 10px;
      margin-right: 6px;
      color: var(--muted);
      transition: transform .2s;
    }
    .expand-indicator.open { transform: rotate(90deg); }

    /* ── Footer ──────────────────────────────────────────────── */
    footer {
      background: var(--surface);
      padding: 24px 48px;
      text-align: center;
      font-size: 12px;
      color: var(--on-dark-m);
      margin-top: 48px;
    }

  </style>
</head>
<body>

  <header>
    <div class="header-brand">
      <div class="corner-sq"></div>
      <span class="header-title">MQTT AI Battle</span>
    </div>
    <div class="header-right">
      <span id="run-status-badge" class="idle">Idle</span>
    </div>
  </header>

  <div id="chaos-banner"></div>

  <main>

    <!-- ── Control Panel ─────────────────────────────────────── -->
    <div class="card" id="control-panel">
      <div class="card-corner"></div>
      <div class="card-title">Start a Run</div>
      <div class="control-row">
        <div class="field">
          <label for="device-count">Devices</label>
          <input type="number" id="device-count" value="3" min="1" max="100">
        </div>
        <div class="field">
          <label for="message-interval">Interval (ms)</label>
          <input type="number" id="message-interval" value="250" min="1">
        </div>
        <div class="field">
          <label for="run-window">Window (s)</label>
          <input type="number" id="run-window" value="120" min="1">
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <div class="check-field">
            <input type="checkbox" id="chaos-schedule" onchange="onScheduleToggle()">
            <label for="chaos-schedule">Chaos Schedule</label>
          </div>
          <div class="check-field" style="margin-top:6px">
            <input type="checkbox" id="chaos-mode" onchange="onManualChaosToggle()">
            <label for="chaos-mode">Manual Chaos Events</label>
          </div>
        </div>
        <div class="field">
          <label>&nbsp;</label>
          <div style="display:flex;gap:8px">
            <button id="start-btn" class="btn btn-primary" onclick="startRun()">▶ Start Run</button>
            <button id="stop-btn" class="btn btn-danger" onclick="stopRun()" style="display:none">⏹ Stop</button>
          </div>
        </div>
      </div>

      <div id="run-timer">
        <div class="timer-label">
          <span class="timer-elapsed" id="timer-elapsed">0s elapsed</span>
          <span id="timer-remaining">—</span>
        </div>
        <div class="progress-track">
          <div class="progress-fill" id="progress-fill" style="width:0%"></div>
        </div>
      </div>

      <div id="start-feedback"></div>

      <div id="schedule-preview">
        <table>
          <thead><tr><th>Offset</th><th>Action</th><th>Type</th><th>Description</th></tr></thead>
          <tbody id="schedule-body"><tr><td colspan="4" class="td-muted">Loading…</td></tr></tbody>
        </table>
      </div>
    </div>

    <!-- ── Organizer Panel ───────────────────────────────────── -->
    <div id="organizer-panel">
      <div class="organizer-title">🎛 Organizer — Manual Chaos Controls</div>
      <div class="organizer-row">
        <span style="font-size:13px;color:#6b7280;margin-right:4px">Key:</span>
        <input type="password" id="organizer-key-input" placeholder="X-Organizer-Key (optional)" oninput="saveOrganizerKey(this.value)" style="padding:6px 10px;border:1px solid #d1d5db;border-radius:2px;font-size:13px;width:220px">
      </div>
      <div class="organizer-row">
        <button class="chaos-btn" onclick="fireChaosEvent('message-duplications','Duplicate messages injected')">💥 Message Duplications</button>
        <button class="chaos-btn" onclick="fireChaosEvent('publisher-disconnect','Forcing publisher to reconnect')">🔌 Publisher Disconnect</button>
        <button class="chaos-btn" onclick="fireChaosEvent('processor-disconnect','Forcing processor to reconnect')">🔌 Processor Disconnect</button>
        <button id="end-event-btn" onclick="endChaosEvent()">🧹 Clear Chaos</button>
      </div>
      <div id="chaos-feedback"></div>
    </div>

    <!-- ── Leaderboard ───────────────────────────────────────── -->
    <div class="section-eyebrow">Live Results</div>
    <div class="section-heading">Leaderboard</div>
    <p class="section-sub">Aggregate results scored in real time by the Judge.</p>
    <div class="card" style="padding:0;overflow:hidden">
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
            <th>Publish %</th>
            <th>Publisher mismatch windows</th>
            <th>Devices</th>
            <th>Interval ms</th>
            <th>Run s</th>
          </tr>
        </thead>
        <tbody id="scores">
          <tr><td colspan="12" class="td-muted" style="padding:20px 12px">Waiting for scores…</td></tr>
        </tbody>
      </table>
    </div>

    <!-- ── Observed Messages ─────────────────────────────────── -->
    <div class="section-eyebrow" style="margin-top:40px">Message Traffic</div>
    <div class="section-heading">Observed Runs</div>
    <p class="section-sub">All MQTT messages seen by the Judge, grouped by run.</p>
    <div class="card" style="padding:0;overflow:hidden">
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Teams</th>
            <th>Messages</th>
            <th>Valid</th>
            <th>Invalid</th>
            <th>Theoretical telemetry</th>
            <th>Telemetry seen</th>
            <th>Publish %</th>
            <th>Results</th>
            <th>Control</th>
            <th>Last Update</th>
          </tr>
        </thead>
        <tbody id="runs">
          <tr><td colspan="11" class="td-muted" style="padding:20px 12px">Waiting for Judge data…</td></tr>
        </tbody>
      </table>
    </div>

  </main>

  <footer>
    MQTT AI Battle &nbsp;·&nbsp; Judge-scored telemetry competition &nbsp;·&nbsp; Refreshes every 2 s
  </footer>

  <script>
    function esc(val) {
      const el = document.createElement('span');
      el.textContent = val ?? '';
      return el.textContent;
    }

    function setControlsEnabled(enabled) {
      ['device-count', 'message-interval', 'run-window', 'chaos-mode', 'chaos-schedule', 'start-btn'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.disabled = !enabled;
      });
      const stopBtn = document.getElementById('stop-btn');
      if (stopBtn) stopBtn.style.display = enabled ? 'none' : 'inline-flex';
    }

    function updateRunStatusBadge(status) {
      const badge = document.getElementById('run-status-badge');
      const normalized = (status || 'idle').toLowerCase();
      badge.className = normalized;
      badge.id = 'run-status-badge';
      badge.textContent = normalized.charAt(0).toUpperCase() + normalized.slice(1);
      setControlsEnabled(normalized === 'idle');
    }

    // ── Run timer ─────────────────────────────────────────────
    let timerInterval = null;
    let runStartedAt = null;
    let runWindowSecs = null;

    function startTimer(startedAtUtc, windowSecs) {
      runStartedAt = new Date(startedAtUtc);
      runWindowSecs = windowSecs;
      document.getElementById('run-timer').classList.add('visible');
      if (timerInterval) clearInterval(timerInterval);
      timerInterval = setInterval(tickTimer, 500);
      tickTimer();
    }

    function stopTimer() {
      if (timerInterval) { clearInterval(timerInterval); timerInterval = null; }
      runStartedAt = null; runWindowSecs = null;
      document.getElementById('run-timer').classList.remove('visible');
      document.getElementById('progress-fill').style.width = '0%';
    }

    function tickTimer() {
      if (!runStartedAt || !runWindowSecs) return;
      const elapsed = Math.max(0, (Date.now() - runStartedAt.getTime()) / 1000);
      const remaining = Math.max(0, runWindowSecs - elapsed);
      const pct = Math.min(100, (elapsed / runWindowSecs) * 100);
      document.getElementById('timer-elapsed').textContent = Math.floor(elapsed) + 's elapsed';
      document.getElementById('timer-remaining').textContent = Math.ceil(remaining) + 's remaining';
      document.getElementById('progress-fill').style.width = pct.toFixed(1) + '%';
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
          stopTimer();
          updateRunStatusBadge('idle');
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
      const chaosScheduleEnabled = document.getElementById('chaos-schedule').checked;
      const feedback = document.getElementById('start-feedback');

      if (!deviceCount || deviceCount < 1) { feedback.className='error'; feedback.textContent='Device Count must be at least 1.'; return; }
      if (!intervalMs || intervalMs < 1)   { feedback.className='error'; feedback.textContent='Message Interval must be at least 1 ms.'; return; }
      if (!runWindowSeconds || runWindowSeconds < 1) { feedback.className='error'; feedback.textContent='Run Window must be at least 1 second.'; return; }

      setControlsEnabled(false);
      feedback.className = '';
      feedback.textContent = 'Starting run…';

      try {
        const res = await fetch('/api/run/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ deviceCount, intervalMs, runWindowSeconds, chaosEnabled, chaosScheduleEnabled })
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

      if (chaosResult.status === 'fulfilled') updateChaosBanner(chaosResult.value);

      let activeRunId = null;
      if (statusResult.status === 'fulfilled' && statusResult.value) {
        const s = statusResult.value;
        updateRunStatusBadge(s.status);
        if (s.status === 'Running' && s.startedAtUtc && s.config?.runWindowSeconds) {
          if (!runStartedAt) startTimer(s.startedAtUtc, s.config.runWindowSeconds);
          activeRunId = s.config.runId ?? null;
        } else if (s.status === 'Idle') {
          stopTimer();
        }
      }
      syncActiveRun(activeRunId);

        if (scoresResult.status === 'fulfilled') {
          const scores = scoresResult.value;
          const scoresBody = document.getElementById('scores');
          if (!scores.length) {
          scoresBody.innerHTML = '<tr><td colspan="12" class="td-muted" style="padding:20px 12px">No scores yet.</td></tr>';
        } else {
          scoresBody.innerHTML = scores.flatMap((score, i) => {
            const rank = i + 1;
            const medal = rank === 1 ? '🥇 ' : rank === 2 ? '🥈 ' : rank === 3 ? '🥉 ' : `${rank}. `;
            const key = scoreKey(score);
            const open = isScoreExpanded(score, activeRunId);
            return [
              `<tr class="expand-row" onclick="toggleScore(this,'${esc(key)}')" data-key="${esc(key)}">
                <td class="td-name" title="${esc(score.runId)}"><span class="expand-indicator${open?' open':''}">▶</span>${medal}${esc(score.name || score.runId)}</td>
                <td>${esc(score.teamId)}</td>
                <td class="td-score">${score.score.toFixed(0)} <span style="font-weight:400;color:var(--muted);font-size:12px">/ 5000</span></td>
                <td>${score.correct}</td>
                <td class="${score.invalid ? 'td-bad' : ''}">${score.invalid}</td>
                <td class="${score.missing ? 'td-bad' : ''}">${score.missing}</td>
                <td>${score.latencyP95Ms.toFixed(0)}</td>
                <td>${pct(score.publishAttainment)}</td>
                <td class="${score.publisherMismatchWindowCount ? 'td-bad' : ''}">${score.publisherMismatchWindowCount}</td>
                <td>${score.deviceCount ?? '—'}</td>
                <td>${score.messageIntervalMs ?? '—'}</td>
                <td>${score.runWindowSeconds ?? '—'}</td>
              </tr>`,
              `<tr class="score-detail-row" id="detail-score-${esc(key)}" style="display:${open?'':'none'}">
                <td colspan="12" style="padding:0">${renderBars(score)}</td>
              </tr>`
            ];
          }).join('');
        }
      }

      if (runsResult.status === 'fulfilled') {
        const runs = runsResult.value;
        const body = document.getElementById('runs');
        if (!runs.length) {
          body.innerHTML = '<tr><td colspan="11" class="td-muted" style="padding:20px 12px">No runs observed yet.</td></tr>';
        } else {
          body.innerHTML = runs.map(run => `<tr>
            <td class="td-name" title="${esc(run.runId)}">${esc(run.name || run.runId)}</td>
            <td>${run.teamIds.map(esc).join(', ')}</td>
            <td>${run.messageCount}</td>
            <td>${run.validMessageCount}</td>
            <td class="${run.invalidMessageCount ? 'td-bad' : ''}">${run.invalidMessageCount}</td>
            <td>${run.theoreticalTelemetryCount ?? '—'}</td>
            <td>${run.telemetryMessageCount}</td>
            <td>${pct(run.publishAttainment)}</td>
            <td>${run.resultMessageCount}</td>
            <td>${run.controlMessageCount}</td>
            <td style="color:var(--muted);font-size:13px">${new Date(run.lastUpdatedUtc).toLocaleString()}</td>
          </tr>`).join('');
        }
      }
    }

    // ── Expand / collapse helpers ──────────────────────────────
    const expandedScoreKeys = new Set();
    let lastActiveRunId = null;

    function scoreKey(score) { return score.runId + '|' + score.teamId; }

    function getExpandedKeys(ns) {
      return ns === 'score' ? expandedScoreKeys : new Set();
    }

    function syncActiveRun(activeRunId) {
      if (activeRunId && activeRunId !== lastActiveRunId) {
        // new run started — auto-expand its rows
        expandedScoreKeys.add(activeRunId + '|*');
        lastActiveRunId = activeRunId;
      }
      if (!activeRunId && lastActiveRunId) {
        lastActiveRunId = null;
      }
    }

    function isScoreExpanded(score, activeRunId) {
      const key = scoreKey(score);
      if (expandedScoreKeys.has(key)) return true;
      // auto-expand all rows for the active runId
      if (activeRunId && score.runId === activeRunId) return true;
      return false;
    }

    function toggleScore(row, key) {
      const detailRow = document.getElementById('detail-score-' + key);
      const indicator = row.querySelector('.expand-indicator');
      if (!detailRow) return;
      const opening = detailRow.style.display === 'none';
      detailRow.style.display = opening ? '' : 'none';
      if (indicator) indicator.classList.toggle('open', opening);
      if (opening) expandedScoreKeys.add(key); else expandedScoreKeys.delete(key);
    }

    function barColor(value) {
      // 0 = red (hsl 0), 1000 = green (hsl 120)
      const hue = Math.round(value * 0.12);
      return `hsl(${hue},72%,40%)`;
    }

    function renderBars(score) {
      const cats = [
        { label: 'Interval', value: score.intervalScore },
        { label: 'Devices', value: score.deviceScore },
        { label: 'Publish Attainment', value: score.publishAttainmentScore },
        { label: 'Window Correctness', value: score.windowCorrectnessScore },
        { label: 'Latency P95', value: score.latencyScore },
      ];
      const rows = cats.map(c => {
        const v = Math.max(0, Math.min(1000, c.value || 0));
        const pct = (v / 10).toFixed(1);
        return `<div class="bar-label">${c.label}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${barColor(v)}"></div></div>
          <div class="bar-value">${v.toFixed(0)}</div>`;
      }).join('');
      return `<div class="score-breakdown">${rows}</div>`;
    }

    function pct(value) {
      return `${(Math.max(0, Math.min(1, value || 0)) * 100).toFixed(1)}%`;
    }

    function updateChaosBanner(chaos) {
      const banner = document.getElementById('chaos-banner');
      banner.className = '';
      banner.textContent = '';
      if (!chaos || !chaos.enabled) {
        document.getElementById('organizer-panel').style.display = document.getElementById('chaos-mode').checked ? 'block' : 'none';
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

    function onManualChaosToggle() {
      const panel = document.getElementById('organizer-panel');
      const checked = document.getElementById('chaos-mode').checked;
      panel.style.display = checked ? 'block' : 'none';
      if (checked) {
        const saved = sessionStorage.getItem('organizerKey') || '';
        document.getElementById('organizer-key-input').value = saved;
      }
    }

    function saveOrganizerKey(val) { sessionStorage.setItem('organizerKey', val); }

    async function fireChaosEvent(type, description) {
      const key = sessionStorage.getItem('organizerKey') || '';
      const feedback = document.getElementById('chaos-feedback');
      feedback.className = '';
      feedback.textContent = 'Firing ' + type + '…';
      try {
        const headers = { 'Content-Type': 'application/json' };
        if (key) headers['X-Organizer-Key'] = key;
        const res = await fetch('/api/chaos/event/start', { method: 'POST', headers, body: JSON.stringify({ type, description }) });
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
          feedback.textContent = '✅ Chaos ended.';
        } else {
          feedback.className = 'error';
          feedback.textContent = 'Failed: ' + res.status;
        }
      } catch (err) {
        feedback.className = 'error';
        feedback.textContent = 'Network error: ' + err.message;
      }
    }

    async function onScheduleToggle() {
      const enabled = document.getElementById('chaos-schedule').checked;
      const preview = document.getElementById('schedule-preview');
      if (!enabled) { preview.classList.remove('visible'); return; }
      preview.classList.add('visible');
      const tbody = document.getElementById('schedule-body');
      try {
        const res = await fetch('/api/chaos/schedule');
        const entries = await res.json();
        tbody.innerHTML = entries.map(e => `<tr>
          <td>T+${e.offsetSeconds}s</td>
          <td>${e.action === 'start' ? '▶ Start' : '⏹ End'}</td>
          <td>${e.eventType ? esc(e.eventType) : '—'}</td>
          <td style="color:var(--muted)">${e.description ? esc(e.description) : '—'}</td>
        </tr>`).join('');
      } catch {
        tbody.innerHTML = '<tr><td colspan="4" class="td-muted">Failed to load schedule.</td></tr>';
      }
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
""";
}
