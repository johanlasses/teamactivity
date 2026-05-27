using System.Diagnostics.Metrics;
using System.Threading.Channels;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Processor;

public sealed class ResultPublishPool : IAsyncDisposable
{
    private static readonly Counter<long> ResultsPublished =
        TelemetryMeters.Processor.CreateCounter<long>("results_published_total");
    private static readonly Counter<long> ReconnectsTotal =
        TelemetryMeters.Processor.CreateCounter<long>("publish_reconnects_total");

    private readonly IMqttClient[] _clients;
    private readonly int _connectionCount;
    private readonly string _host;
    private readonly int _port;
    private readonly string _teamId;
    private readonly ILogger _logger;
    private readonly int[] _reconnectInProgress;

    public ResultPublishPool(int connectionCount, string host, int port, string teamId, ILogger logger)
    {
        _connectionCount = connectionCount;
        _host = host;
        _port = port;
        _teamId = teamId;
        _logger = logger;
        _clients = new IMqttClient[connectionCount];
        _reconnectInProgress = new int[connectionCount];

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
                if (Interlocked.CompareExchange(ref _reconnectInProgress[index], 1, 0) != 0) return;

                try
                {
                    ReconnectsTotal.Add(1, new KeyValuePair<string, object?>("connection", index));
                    _logger.LogWarning("Publish connection {Index} disconnected: {Reason}. Reconnecting...",
                        index, args.Reason);
                    await ReconnectWithBackoff(client, index, cancellationToken);
                }
                finally
                {
                    Volatile.Write(ref _reconnectInProgress[index], 0);
                }
            };

            var options = new MqttClientOptionsBuilder()
                .WithClientId($"processor-pub-{_teamId}-{i}")
                .WithTcpServer(_host, _port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
                .WithCleanSession()
                .Build();

            connectTasks[i] = client.ConnectAsync(options, cancellationToken);
        }

        await Task.WhenAll(connectTasks);
        _logger.LogInformation("All {Count} result publish connections established", _connectionCount);
    }

    public Task[] StartDrainLoops(ChannelReader<MqttApplicationMessage> reader, CancellationToken cancellationToken)
    {
        var tasks = new Task[_connectionCount];
        for (int i = 0; i < _connectionCount; i++)
        {
            var client = _clients[i];
            var connIndex = i;

            tasks[i] = Task.Run(async () =>
            {
                await foreach (var msg in reader.ReadAllAsync(cancellationToken))
                {
                    for (int retry = 0; retry < 3; retry++)
                    {
                        try
                        {
                            await client.PublishAsync(msg, cancellationToken);
                            ResultsPublished.Add(1);
                            break;
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception ex) when (retry < 2)
                        {
                            _logger.LogDebug(ex, "Result publish attempt {Attempt} failed on connection {Index}, retrying...", retry + 1, connIndex);
                            try { await Task.Delay(50 * (retry + 1), cancellationToken); }
                            catch (OperationCanceledException) { return; }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Result publish failed permanently on connection {Index} after 3 attempts", connIndex);
                        }
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
                    .WithClientId($"processor-pub-{_teamId}-{index}")
                    .WithTcpServer(_host, _port)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
                    .WithCleanSession()
                    .Build();

                await client.ConnectAsync(options, cancellationToken);
                _logger.LogInformation("Publish connection {Index} reconnected after {Attempts} attempts", index, attempt + 1);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                attempt++;
                _logger.LogDebug(ex, "Publish connection {Index} reconnect attempt {Attempt} failed", index, attempt);
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
