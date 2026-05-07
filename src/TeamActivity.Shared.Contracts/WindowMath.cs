namespace TeamActivity.Shared.Contracts;

public static class WindowMath
{
    public static WindowKey Assign(TelemetryMessage telemetry, int windowSeconds)
    {
        var windowStart = GetWindowStart(telemetry.EventTimeUtc, windowSeconds);
        return new WindowKey(
            telemetry.RunId,
            telemetry.TeamId,
            telemetry.DeviceId,
            windowStart,
            windowStart.AddSeconds(windowSeconds));
    }

    public static DateTimeOffset GetWindowStart(DateTimeOffset eventTimeUtc, int windowSeconds)
    {
        if (windowSeconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(windowSeconds), "Window size must be greater than zero.");
        }

        var eventUnixMs = eventTimeUtc.ToUniversalTime().ToUnixTimeMilliseconds();
        var windowMs = windowSeconds * 1000L;
        var windowStartUnixMs = eventUnixMs - eventUnixMs % windowMs;
        return DateTimeOffset.FromUnixTimeMilliseconds(windowStartUnixMs);
    }

    public static string TelemetryDedupeKey(TelemetryMessage telemetry)
        => $"{telemetry.RunId}|{telemetry.TeamId}|{telemetry.DeviceId}|{telemetry.Sequence}";

    public static string ResultId(WindowKey key)
        => $"{key.TeamId}-{key.DeviceId}-{key.WindowStartUtc:yyyyMMddTHHmmssfffZ}";
}
