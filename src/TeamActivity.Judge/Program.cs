using TeamActivity.Judge;
using TeamActivity.Shared.Contracts;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.Configure<MqttOptions>(builder.Configuration.GetSection("Mqtt"));
builder.Services.Configure<ChallengeOptions>(builder.Configuration.GetSection("Challenge"));
builder.Services.AddSingleton<ObservedRunStore>();
builder.Services.AddSingleton<ScoringStore>();
builder.Services.AddSingleton<ChaosStore>();
builder.Services.AddSingleton<RunTriggerStore>();
builder.Services.AddHostedService<JudgeWorker>();

var app = builder.Build();

var organizerKey = builder.Configuration["ORGANIZER_KEY"] ?? string.Empty;

app.MapDefaultEndpoints();
app.MapGet("/", () => Results.Text("MQTT Telemetry Gauntlet Judge is running."));
app.MapGet("/api/runs", (ObservedRunStore store) => store.GetRuns());
app.MapGet("/api/runs/{runId}/messages", (string runId, ObservedRunStore store) => store.GetMessages(runId));
app.MapGet("/api/scores", (ScoringStore store) => store.GetScores());
app.MapGet("/api/runs/{runId}/scores", (string runId, ScoringStore store) => store.GetScores(runId));

app.MapGet("/api/chaos", (ChaosStore chaos) => Results.Json(chaos.GetState()));

app.MapGet("/api/run/status", (RunTriggerStore triggers) => Results.Ok(triggers.GetStatus()));

app.MapGet("/api/run/pending", (RunTriggerStore triggers) =>
{
    var config = triggers.GetPending();
    return config is null ? Results.NoContent() : Results.Ok(config);
});

app.MapPost("/api/run/acknowledge", (AcknowledgeRunRequest req, RunTriggerStore triggers) =>
{
    return triggers.TryAcknowledge(req.RunId)
        ? Results.Ok()
        : Results.Conflict("Run acknowledgement failed — state may have changed.");
});

app.MapPost("/api/run/start", (RunStartRequest req, RunTriggerStore triggers, ChaosStore chaos, ScoringStore scoring) =>
{
    if (req.DeviceCount <= 0 || req.IntervalMs <= 0 || req.RunWindowSeconds <= 0)
        return Results.BadRequest("DeviceCount, IntervalMs, and RunWindowSeconds must be greater than 0.");

    var config = new RunTriggerConfig(
        Guid.NewGuid().ToString(),
        RunNames.Random(),
        req.DeviceCount,
        req.IntervalMs,
        req.RunWindowSeconds,
        req.ChaosEnabled);

    if (!triggers.TrySetPending(config))
        return Results.Conflict("A run is already pending or in progress. Wait for it to complete.");

    scoring.RegisterRunName(config.RunId, config.Name);

    if (req.ChaosEnabled)
        chaos.Enable(config.RunId);
    else
        chaos.Disable();

    return Results.Ok(config);
});

app.MapPost("/api/run/stop", (RunTriggerStore triggers, ChaosStore chaos) =>
{
    var cancelled = triggers.TryCancel();
    chaos.Disable();
    return cancelled ? Results.Ok() : Results.Conflict("No run in progress.");
});

app.MapPost("/api/chaos/enable", (ChaosEnableRequest req, ChaosStore chaos, HttpContext ctx) =>
{
    if (!IsAuthorized(ctx, organizerKey)) return Results.Unauthorized();
    chaos.Enable(req.RunId);
    return Results.Ok(chaos.GetState());
});

app.MapPost("/api/chaos/disable", (ChaosStore chaos, HttpContext ctx) =>
{
    if (!IsAuthorized(ctx, organizerKey)) return Results.Unauthorized();
    chaos.Disable();
    return Results.Ok(chaos.GetState());
});

app.MapPost("/api/chaos/event/start", (ChaosEventStartRequest req, ChaosStore chaos, HttpContext ctx) =>
{
    if (!IsAuthorized(ctx, organizerKey)) return Results.Unauthorized();
    if (!ChaosStore.AllowedEventTypes.Contains(req.Type))
    {
        return Results.BadRequest(
            $"Unknown event type '{req.Type}'. Allowed: {string.Join(", ", ChaosStore.AllowedEventTypes)}");
    }
    chaos.StartEvent(req.Type, req.Description ?? string.Empty);
    return Results.Ok(chaos.GetState());
});

app.MapPost("/api/chaos/event/end", (ChaosStore chaos, HttpContext ctx) =>
{
    if (!IsAuthorized(ctx, organizerKey)) return Results.Unauthorized();
    chaos.EndEvent();
    return Results.Ok(chaos.GetState());
});

app.Run();

static bool IsAuthorized(HttpContext ctx, string requiredKey)
{
    if (string.IsNullOrEmpty(requiredKey)) return true;
    return ctx.Request.Headers.TryGetValue("X-Organizer-Key", out var key) && key == requiredKey;
}

internal sealed record ChaosEnableRequest(string RunId);
internal sealed record ChaosEventStartRequest(string Type, string? Description);
internal sealed record RunStartRequest(int DeviceCount, int IntervalMs, int RunWindowSeconds, bool ChaosEnabled);
internal sealed record AcknowledgeRunRequest(string RunId);
