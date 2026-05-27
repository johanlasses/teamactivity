using System.Buffers;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading.Channels;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Publisher;

public sealed class ConnectionPool : IAsyncDisposable
{
    private static readonly JsonEncodedText SchemaVersionProperty = JsonEncodedText.Encode("schemaVersion");
    private static readonly JsonEncodedText RunIdProperty = JsonEncodedText.Encode("runId");
    private static readonly JsonEncodedText TeamIdProperty = JsonEncodedText.Encode("teamId");
    private static readonly JsonEncodedText DeviceIdProperty = JsonEncodedText.Encode("deviceId");
    private static readonly JsonEncodedText SequenceProperty = JsonEncodedText.Encode("sequence");
    private static readonly JsonEncodedText EventTimeUtcProperty = JsonEncodedText.Encode("eventTimeUtc");
    private static readonly JsonEncodedText PublishedAtUtcProperty = JsonEncodedText.Encode("publishedAtUtc");
    private static readonly JsonEncodedText ValueProperty = JsonEncodedText.Encode("value");

    private static readonly Counter<long> ReconnectsTotal =
        TelemetryMeters.Publisher.CreateCounter<long>("connection_reconnects_total");

    private readonly IMqttClient[] _clients;
    private readonly int _connectionCount;
    private readonly string _host;
    private readonly int _port;
    private readonly string _teamId;
    private readonly ILogger _logger;
    private readonly int _maxPublishRetryAttempts;

    public ConnectionPool(int connectionCount, string host, int port, string teamId, ILogger logger, int maxPublishRetryAttempts)
    {
        _connectionCount = connectionCount;
        _host = host;
        _port = port;
        _teamId = teamId;
        _logger = logger;
        _maxPublishRetryAttempts = Math.Max(1, maxPublishRetryAttempts);
        _clients = new IMqttClient[connectionCount];

        var factory = new MqttClientFactory();
        for (int i = 0; i < connectionCount; i++)
        {
            _clients[i] = factory.CreateMqttClient();
        }
    }

    public async Task ConnectAllAsync(CancellationToken cancellationToken)
    {
        var connectTasks = new Task[_connectionCount];
        for (int i = 0; i < _connectionCount; i++)
        {
            var index = i;
            var client = _clients[i];

            client.DisconnectedAsync += async args =>
            {
                if (cancellationToken.IsCancellationRequested) return;
                ReconnectsTotal.Add(1, new KeyValuePair<string, object?>("connection", index));
                _logger.LogWarning("Pool connection {Index} disconnected: {Reason}", index, args.Reason);
                await ReconnectWithBackoff(client, index, cancellationToken);
            };

            var clientId = $"publisher-{_teamId}-pool-{index}";
            var options = new MqttClientOptionsBuilder()
                .WithClientId(clientId)
                .WithTcpServer(_host, _port)
                .WithProtocolVersion(MqttProtocolVersion.V311)
                .WithCleanSession()
                .Build();

            connectTasks[i] = client.ConnectAsync(options, cancellationToken);
        }

        await Task.WhenAll(connectTasks);
        _logger.LogInformation("All {Count} pool connections established", _connectionCount);
    }

    public Task[] StartDrainLoops(Channel<TelemetryEnvelope>[] shards, string telemetryTopic, CancellationToken cancellationToken)
    {
        var tasks = new Task[shards.Length];
        for (int i = 0; i < shards.Length; i++)
        {
            var shardIndex = i;
            var connectionIndex = i % _connectionCount;
            var client = _clients[connectionIndex];
            var reader = shards[i].Reader;

            tasks[i] = Task.Run(async () =>
            {
                var payloadBuffer = new ArrayBufferWriter<byte>(256);
                using var jsonWriter = new Utf8JsonWriter(payloadBuffer);

                await foreach (var telemetry in reader.ReadAllAsync(cancellationToken))
                {
                    try
                    {
                        var message = SerializeTelemetry(telemetryTopic, telemetry, payloadBuffer, jsonWriter);
                        await PublishWithRetryAsync(client, message, shardIndex, cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                }
            }, cancellationToken);
        }

        return tasks;
    }

    private async Task ReconnectWithBackoff(IMqttClient client, int index, CancellationToken cancellationToken)
    {
        var baseDelayMs = 100;
        var maxDelayMs = 5000;
        var attempt = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var jitter = Random.Shared.Next(0, 100);
                var delayMs = Math.Min(baseDelayMs * (1 << attempt), maxDelayMs) + jitter;
                await Task.Delay(delayMs, cancellationToken);

                var options = new MqttClientOptionsBuilder()
                    .WithClientId($"publisher-{_teamId}-pool-{index}")
                    .WithTcpServer(_host, _port)
                    .WithProtocolVersion(MqttProtocolVersion.V311)
                    .WithCleanSession()
                    .Build();

                await client.ConnectAsync(options, cancellationToken);
                _logger.LogInformation("Pool connection {Index} reconnected after {Attempts} attempts", index, attempt + 1);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                attempt++;
                _logger.LogDebug(ex, "Pool connection {Index} reconnect attempt {Attempt} failed", index, attempt);
            }
        }
    }

    private async Task PublishWithRetryAsync(
        IMqttClient client,
        MqttApplicationMessage message,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                while (!client.IsConnected && !cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(20, cancellationToken);
                }

                await client.PublishAsync(message, cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                attempt++;
                if (attempt >= _maxPublishRetryAttempts)
                {
                    _logger.LogWarning(ex,
                        "Dropping message on shard {Shard} after {Attempts} failed publish attempts",
                        shardIndex, attempt);
                    return;
                }

                var delayMs = Math.Min(1000, 25 * attempt);
                _logger.LogDebug(ex,
                    "Publish retry on shard {Shard} failed (attempt {Attempt}), delaying {DelayMs}ms",
                    shardIndex, attempt, delayMs);
                await Task.Delay(delayMs, cancellationToken);
            }
        }
    }

    private static MqttApplicationMessage SerializeTelemetry(
        string topic,
        in TelemetryEnvelope telemetry,
        ArrayBufferWriter<byte> payloadBuffer,
        Utf8JsonWriter jsonWriter)
    {
        payloadBuffer.Clear();
        jsonWriter.Reset(payloadBuffer);

        jsonWriter.WriteStartObject();
        jsonWriter.WriteNumber(SchemaVersionProperty, Topics.SchemaVersion);
        jsonWriter.WriteString(RunIdProperty, telemetry.RunId);
        jsonWriter.WriteString(TeamIdProperty, telemetry.TeamId);
        jsonWriter.WriteString(DeviceIdProperty, telemetry.DeviceId);
        jsonWriter.WriteNumber(SequenceProperty, telemetry.Sequence);
        jsonWriter.WriteString(EventTimeUtcProperty, telemetry.EventTimeUtc);
        jsonWriter.WriteString(PublishedAtUtcProperty, telemetry.PublishedAtUtc);
        jsonWriter.WriteNumber(ValueProperty, telemetry.Value);
        jsonWriter.WriteEndObject();
        jsonWriter.Flush();

        if (!MemoryMarshal.TryGetArray(payloadBuffer.WrittenMemory, out var payloadSegment))
        {
            payloadSegment = new ArraySegment<byte>(payloadBuffer.WrittenSpan.ToArray());
        }

        return new MqttApplicationMessage
        {
            Topic = topic,
            QualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce,
            PayloadSegment = payloadSegment
        };
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var client in _clients)
        {
            try
            {
                if (client.IsConnected)
                {
                    await client.DisconnectAsync(new MqttClientDisconnectOptionsBuilder().Build());
                }
            }
            catch { }
            client.Dispose();
        }
    }
}
