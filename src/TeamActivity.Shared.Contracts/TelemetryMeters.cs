using System.Diagnostics.Metrics;

namespace TeamActivity.Shared.Contracts;

public static class TelemetryMeters
{
    public static readonly Meter Publisher = new("TeamActivity.Publisher");

    public static readonly Meter Processor = new("TeamActivity.Processor");

    public static readonly Meter Judge = new("TeamActivity.Judge");

    public static readonly Meter Scoreboard = new("TeamActivity.Scoreboard");
}
