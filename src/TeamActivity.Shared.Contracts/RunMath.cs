namespace TeamActivity.Shared.Contracts;

public static class RunMath
{
    public static int CalculateMessagesPerDevice(int runWindowSeconds, int intervalMs)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(runWindowSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(intervalMs);

        var runWindowMs = runWindowSeconds * 1000L;
        return (int)((runWindowMs - 1) / intervalMs + 1);
    }

    public static long CalculateTheoreticalTelemetryCount(int deviceCount, int runWindowSeconds, int intervalMs)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(deviceCount);
        return (long)deviceCount * CalculateMessagesPerDevice(runWindowSeconds, intervalMs);
    }

    public static IReadOnlyList<WindowExpectation> BuildWindowExpectations(
        DateTimeOffset runStartUtc,
        int runWindowSeconds,
        int intervalMs,
        int windowSeconds)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(runWindowSeconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(intervalMs);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(windowSeconds);

        var expectations = new List<WindowExpectation>();
        var runEndUtc = runStartUtc.AddSeconds(runWindowSeconds);
        var windowStartUtc = WindowMath.GetWindowStart(runStartUtc, windowSeconds);

        while (windowStartUtc < runEndUtc)
        {
            var windowEndUtc = windowStartUtc.AddSeconds(windowSeconds);
            var expectedCount = CountScheduledMessagesInWindow(runStartUtc, runEndUtc, intervalMs, windowStartUtc, windowEndUtc);
            if (expectedCount > 0)
            {
                expectations.Add(new WindowExpectation(windowStartUtc, windowEndUtc, expectedCount));
            }

            windowStartUtc = windowEndUtc;
        }

        return expectations;
    }

    public static int CountScheduledMessagesInWindow(
        DateTimeOffset runStartUtc,
        DateTimeOffset runEndUtc,
        int intervalMs,
        DateTimeOffset windowStartUtc,
        DateTimeOffset windowEndUtc)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(intervalMs);
        if (runEndUtc <= runStartUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(runEndUtc), "Run end must be after run start.");
        }

        if (windowEndUtc <= windowStartUtc)
        {
            throw new ArgumentOutOfRangeException(nameof(windowEndUtc), "Window end must be after window start.");
        }

        var effectiveStartUtc = windowStartUtc > runStartUtc ? windowStartUtc : runStartUtc;
        var effectiveEndUtc = windowEndUtc < runEndUtc ? windowEndUtc : runEndUtc;
        if (effectiveStartUtc >= effectiveEndUtc)
        {
            return 0;
        }

        var runStartMs = runStartUtc.ToUnixTimeMilliseconds();
        var startOffsetMs = effectiveStartUtc.ToUnixTimeMilliseconds() - runStartMs;
        var endOffsetMs = effectiveEndUtc.ToUnixTimeMilliseconds() - runStartMs;

        var firstIndex = (startOffsetMs + intervalMs - 1) / intervalMs;
        var lastIndex = (endOffsetMs - 1) / intervalMs;

        return lastIndex < firstIndex
            ? 0
            : (int)(lastIndex - firstIndex + 1);
    }
}

public sealed record WindowExpectation(
    DateTimeOffset WindowStartUtc,
    DateTimeOffset WindowEndUtc,
    int ExpectedCountPerDevice);
