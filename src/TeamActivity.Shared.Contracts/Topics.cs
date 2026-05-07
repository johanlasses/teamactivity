namespace TeamActivity.Shared.Contracts;

public static class Topics
{
    public const int SchemaVersion = 2;

    public const string PublisherStart = "publisher-start";

    public const string PublisherComplete = "publisher-complete";

    public static string TelemetryRaw(string runId, string teamId)
        => $"telemetry/v1/{runId}/{teamId}/raw";

    public static string Result(string runId, string teamId, string deviceId, DateTimeOffset windowStartUtc)
        => $"results/v1/{runId}/{teamId}/device/{deviceId}/window/{windowStartUtc.ToUnixTimeMilliseconds()}";

    public static string Control(string runId, string teamId, string eventName)
        => $"control/v1/{runId}/{teamId}/{eventName}";

    public static bool TryParseTelemetryRaw(string topic, out string runId, out string teamId)
    {
        runId = string.Empty;
        teamId = string.Empty;

        var segments = topic.Split('/');
        if (segments is ["telemetry", "v1", var parsedRunId, var parsedTeamId, "raw"])
        {
            runId = parsedRunId;
            teamId = parsedTeamId;
            return true;
        }

        return false;
    }

    public static bool TryParseControl(string topic, out string runId, out string teamId, out string eventName)
    {
        runId = string.Empty;
        teamId = string.Empty;
        eventName = string.Empty;

        var segments = topic.Split('/');
        if (segments is ["control", "v1", var parsedRunId, var parsedTeamId, var parsedEvent])
        {
            runId = parsedRunId;
            teamId = parsedTeamId;
            eventName = parsedEvent;
            return true;
        }

        return false;
    }

    public static bool TryParseResult(string topic, out string runId, out string teamId, out string deviceId, out long windowStartUnixMs)
    {
        runId = string.Empty;
        teamId = string.Empty;
        deviceId = string.Empty;
        windowStartUnixMs = 0;

        var segments = topic.Split('/');
        if (segments is ["results", "v1", var parsedRunId, var parsedTeamId, "device", var parsedDeviceId, "window", var parsedWindow]
            && long.TryParse(parsedWindow, out var parsedWindowStartUnixMs))
        {
            runId = parsedRunId;
            teamId = parsedTeamId;
            deviceId = parsedDeviceId;
            windowStartUnixMs = parsedWindowStartUnixMs;
            return true;
        }

        return false;
    }
}
