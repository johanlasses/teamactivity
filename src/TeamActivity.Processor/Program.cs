using TeamActivity.Processor;
using TeamActivity.Shared.Contracts;

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();
builder.Services.Configure<MqttOptions>(builder.Configuration.GetSection("Mqtt"));
builder.Services.Configure<ChallengeOptions>(builder.Configuration.GetSection("Challenge"));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
