namespace TeamActivity.Shared.Contracts;

public static class ScoreMath
{
    public static double CalculateIntervalChallenge(int intervalMs, int baselineIntervalMs = 50)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(intervalMs);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(baselineIntervalMs);

        return Math.Min(1d, (double)baselineIntervalMs / intervalMs);
    }

    public static double CalculateDeviceChallenge(int deviceCount, int baselineDeviceCount = 10_000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(deviceCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(baselineDeviceCount);

        return Math.Min(1d, (double)deviceCount / baselineDeviceCount);
    }

    public static double CalculateCorrectVolumeBonus(int correctWindowCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(correctWindowCount);
        return 10 * Math.Log10(1 + correctWindowCount);
    }
}
