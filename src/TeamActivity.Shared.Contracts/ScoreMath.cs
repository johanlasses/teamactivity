namespace TeamActivity.Shared.Contracts;

/// <summary>
/// Scoring 2.0: five categories, each worth 0–1000 points. Maximum total is 5000.
/// </summary>
public static class ScoreMath
{
    // Interval: 50 ms → 1000 pts, 1000 ms → 0 pts (linear)
    private const int IntervalBest = 50;
    private const int IntervalWorst = 1000;

    // Devices: 50 000 devices → 1000 pts, 1 device → ~0 pts (linear)
    private const int DeviceMax = 50_000;

    // Latency: 100 ms P95 → 1000 pts, ≥1000 ms → 0 pts (linear)
    private const int LatencyBest = 100;
    private const int LatencyWorst = 1000;

    public static double CalculateIntervalScore(int intervalMs)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(intervalMs);
        return Math.Max(0d, Math.Min(1000d,
            1000d * (IntervalWorst - intervalMs) / (IntervalWorst - IntervalBest)));
    }

    public static double CalculateDeviceScore(int deviceCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(deviceCount);
        return Math.Max(0d, Math.Min(1000d, 1000d * deviceCount / DeviceMax));
    }

    public static double CalculatePublishAttainmentScore(double publishAttainment)
    {
        return Math.Max(0d, Math.Min(1000d, 1000d * publishAttainment));
    }

    public static double CalculateWindowCorrectnessScore(double windowCorrectness)
    {
        return Math.Max(0d, Math.Min(1000d, 1000d * windowCorrectness));
    }

    public static double CalculateLatencyScore(double latencyP95Ms)
    {
        if (latencyP95Ms <= 0d) return 0d;
        return Math.Max(0d, Math.Min(1000d,
            1000d * (LatencyWorst - latencyP95Ms) / (LatencyWorst - LatencyBest)));
    }
}
