var builder = DistributedApplication.CreateBuilder(args);

var mosquittoConfig = Path.GetFullPath(Path.Combine(
    AppContext.BaseDirectory,
    "..",
    "..",
    "..",
    "mosquitto",
    "mosquitto.conf"));

var mqtt = builder.AddContainer("mosquitto", "eclipse-mosquitto", "2.0")
    .WithBindMount(mosquittoConfig, "/mosquitto/config/mosquitto.conf", isReadOnly: true)
    .WithEndpoint(port: 1883, targetPort: 1883, name: "mqtt", scheme: "tcp");

var judge = builder.AddProject<Projects.TeamActivity_Judge>("judge")
    .WithEnvironment("MQTT__Host", "localhost")
    .WithEnvironment("MQTT__Port", "1883")
    .WaitFor(mqtt);

var processor = builder.AddProject<Projects.TeamActivity_Processor>("processor")
    .WithEnvironment("MQTT__Host", "localhost")
    .WithEnvironment("MQTT__Port", "1883")
    .WaitFor(mqtt);

builder.AddProject<Projects.TeamActivity_Publisher>("publisher")
    .WithEnvironment("MQTT__Host", "localhost")
    .WithEnvironment("MQTT__Port", "1883")
    .WithEnvironment("Publisher__StartupDelaySeconds", "3")
    .WithEnvironment("Publisher__MessageCount", "480")
    .WithEnvironment("Publisher__MessageIntervalMilliseconds", "250")
    .WithEnvironment("Publisher__DeviceCount", "3")
    .WaitFor(mqtt)
    .WaitFor(judge)
    .WaitFor(processor);

builder.AddProject<Projects.TeamActivity_Scoreboard>("scoreboard")
    .WithReference(judge)
    .WaitFor(judge);

builder.Build().Run();
