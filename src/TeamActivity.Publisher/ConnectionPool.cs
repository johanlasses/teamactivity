using System.Diagnostics.Metrics;
using System.Threading.Channels;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Publisher;

/// <summary>
/// Manages a pool of MQTT connections for high-throughput telemetry publishing.
/// Each connection drains assigned shards (shard % ConnectionCount == connectionIndex).
/// </summary>
public sealed class ConnectionPool : IAsyncDisposable
{
    private static readonly Counter<long> ReconnectsTotal =
        TelemetryMeters.Publisher.CreateCounter<long>("connection_reconnects_total");

    private readonly IMqttClient[] _clients;
    private readonly int _connectionCount;
    private readonly string _host;
    private readonly int _port;
    private readonly string _teamId;
    private readonly ILogger _logger;
    private readonly Task[] _drainTasks;

    public ConnectionPool(int connectionCount, string host, int port, string teamId, ILogger logger)
    {
        _connectionCount = connectionCount;
        _host = host;
        _port = port;
        _teamId = teamId;
        _logger = logger;
        _clients = new IMqttClient[connectionCount];
        _drainTasks = new Task[0];

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
                _logger.LogWarning("Pool connection {Index} disconnected: {Reason}. Reconnecting...",
                    index, args.Reason);

                await ReconnectWithBackoff(client, index, cancellationToken);
            };

            var options = new MqttClientOptionsBuilder()
                .WithClientId($"publisher-{_teamId}-pool-{i}")
                .WithTcpServer(_host, _port)
                .WithCleanStart()
                .Build();

            connectTasks[i] = client.ConnectAsync(options, cancellationToken);
        }

        await Task.WhenAll(connectTasks);
        _logger.LogInformation("All {Count} pool connections established", _connectionCount);
    }

    /// <summary>
    /// Starts drain loops. Each shard is drained by connection[shard % ConnectionCount].
    /// Returns tasks for all drain loops (one per shard).
    /// </summary>
    public Task[] StartDrainLoops(Channel<MqttApplicationMessage>[] shards, CancellationToken cancellationToken)
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
                await foreach (var msg in reader.ReadAllAsync(cancellationToken))
                {
                    try
                    {
                        await client.PublishAsync(msg, cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Publish failed on shard {Shard}, will retry on next message", shardIndex);
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
                    .WithCleanStart()
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
