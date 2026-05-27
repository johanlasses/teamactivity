using TeamActivity.Processor;
using TeamActivity.Shared.Contracts;

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();

// Resolve Docker container IP to bypass docker-proxy throughput bottleneck.
var configHost = builder.Configuration["Mqtt:Host"] ?? "localhost";
var configPort = int.TryParse(builder.Configuration["Mqtt:Port"], out var cp) ? cp : 1883;
var (resolvedHost, resolvedPort) = BrokerResolver.Resolve(configHost, configPort);
builder.Configuration["Mqtt:Host"] = resolvedHost;
builder.Configuration["Mqtt:Port"] = resolvedPort.ToString();

builder.Services.Configure<MqttOptions>(builder.Configuration.GetSection("Mqtt"));
builder.Services.Configure<ChallengeOptions>(builder.Configuration.GetSection("Challenge"));
builder.Services.Configure<ProcessorOptions>(builder.Configuration.GetSection("Processor"));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
