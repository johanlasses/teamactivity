using System.Diagnostics;

namespace TeamActivity.Shared.Contracts;

public static class TelemetryActivitySources
{
    public static readonly ActivitySource Judge = new("TeamActivity.Judge");

    public static readonly ActivitySource Publisher = new("TeamActivity.Publisher");
}
