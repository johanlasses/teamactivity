using TeamActivity.Judge;
using TeamActivity.Shared.Contracts;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.Configure<MqttOptions>(builder.Configuration.GetSection("Mqtt"));
builder.Services.Configure<ChallengeOptions>(builder.Configuration.GetSection("Challenge"));
builder.Services.AddSingleton<ObservedRunStore>();
builder.Services.AddSingleton<ScoringStore>();
builder.Services.AddHostedService<JudgeWorker>();

var app = builder.Build();

app.MapDefaultEndpoints();
app.MapGet("/", () => Results.Text("MQTT Telemetry Gauntlet Judge is running."));
app.MapGet("/api/runs", (ObservedRunStore store) => store.GetRuns());
app.MapGet("/api/runs/{runId}/messages", (string runId, ObservedRunStore store) => store.GetMessages(runId));
app.MapGet("/api/scores", (ScoringStore store) => store.GetScores());
app.MapGet("/api/runs/{runId}/scores", (string runId, ScoringStore store) => store.GetScores(runId));
app.Run();
