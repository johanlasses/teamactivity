namespace TeamActivity.Shared.Contracts;

/// <summary>
/// Scoring 2.0: five categories, each worth 0–1000 points. Maximum total is 5000.
/// </summary>
public static class ScoreMath
{
    // Latency: 100 ms P95 → 1000 pts, ≥1000 ms → 0 pts (linear)
    private const int LatencyBest = 100;
    private const int LatencyWorst = 1000;

    /// <summary>
    /// Piecewise-linear curve — steep at low intervals, flattening toward 1000 ms.
    /// Anchors: 50→1000, 100→800, 250→500, 500→250, 750→100, 1000→0.
    /// </summary>
    public static double CalculateIntervalScore(int intervalMs)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(intervalMs);
        return intervalMs switch
        {
            <= 50  => 1000d,
            <= 100 => Lerp(intervalMs, 50,  1000, 100, 800),
            <= 250 => Lerp(intervalMs, 100,  800, 250, 500),
            <= 500 => Lerp(intervalMs, 250,  500, 500, 250),
            <= 750 => Lerp(intervalMs, 500,  250, 750, 100),
            <= 1000 => Lerp(intervalMs, 750, 100, 1000,  0),
            _ => 0d
        };
    }

    /// <summary>
    /// Log-linear below 10 000 devices (anchors: 10→50, 100→100, 1000→200, 10000→500),
    /// then linear from 10 000→500 to 50 000→1000.
    /// </summary>
    public static double CalculateDeviceScore(int deviceCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(deviceCount);
        if (deviceCount >= 50_000) return 1000d;
        if (deviceCount >= 10_000) return Lerp(deviceCount, 10_000, 500, 50_000, 1000);

        var log = Math.Log10(deviceCount);
        return log switch
        {
            >= 3 => Lerp(log, 3, 200, 4, 500),
            >= 2 => Lerp(log, 2, 100, 3, 200),
            >= 1 => Lerp(log, 1,  50, 2, 100),
            _    => Lerp(log, 0,   0, 1,  50)
        };
    }

    public static double CalculatePublishAttainmentScore(double publishAttainment)
        => Math.Max(0d, Math.Min(1000d, 1000d * publishAttainment));

    public static double CalculateWindowCorrectnessScore(double windowCorrectness)
        => Math.Max(0d, Math.Min(1000d, 1000d * windowCorrectness));

    public static double CalculateLatencyScore(double latencyP95Ms)
    {
        if (latencyP95Ms <= 0d) return 0d;
        return Math.Max(0d, Math.Min(1000d,
            1000d * (LatencyWorst - latencyP95Ms) / (LatencyWorst - LatencyBest)));
    }

    private static double Lerp(double x, double x0, double y0, double x1, double y1)
        => y0 + (y1 - y0) * (x - x0) / (x1 - x0);
}
