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

var judge = builder.AddDockerfile("judge", "..", "src/TeamActivity.Judge/Dockerfile")
    .WithHttpEndpoint(port: 5076, targetPort: 8080, name: "http")
    .WithEnvironment("MQTT__Host", "mosquitto")
    .WithEnvironment("MQTT__Port", "1883")
    .WithOtlpExporter()
    .WaitFor(mqtt);

var processor = builder.AddProject<Projects.TeamActivity_Processor>("processor")
    .WithEnvironment("MQTT__Host", "localhost")
    .WithEnvironment("MQTT__Port", "1883")
    .WaitFor(mqtt);

builder.AddProject<Projects.TeamActivity_Publisher>("publisher")
    .WithEnvironment("MQTT__Host", "localhost")
    .WithEnvironment("MQTT__Port", "1883")
    .WithEnvironment("Publisher__StartupDelaySeconds", "3")
    .WaitFor(mqtt)
    .WaitFor(processor);

builder.AddDockerfile("scoreboard", "..", "src/TeamActivity.Scoreboard/Dockerfile")
    .WithHttpEndpoint(port: 5216, targetPort: 8080, name: "http")
    .WithReference(judge.GetEndpoint("http"))
    .WithOtlpExporter()
    .WaitFor(judge);

builder.Build().Run();
