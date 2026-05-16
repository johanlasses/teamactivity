using System.Threading.Channels;

namespace TeamActivity.Judge;

public sealed class RunAnnouncer
{
    private readonly Channel<(string Topic, string Json)> _channel =
        Channel.CreateUnbounded<(string, string)>();

    public ChannelReader<(string Topic, string Json)> Reader => _channel.Reader;

    public void Announce(string topic, string json) => _channel.Writer.TryWrite((topic, json));
}
