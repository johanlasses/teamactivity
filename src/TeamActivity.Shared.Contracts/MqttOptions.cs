namespace TeamActivity.Shared.Contracts;

public sealed class MqttOptions
{
    public string Host { get; init; } = "localhost";

    public int Port { get; init; } = 1883;
}
