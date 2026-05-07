namespace TeamActivity.Shared.Contracts;

public sealed class AggregateWindow
{
    public int Count { get; private set; }

    public double Sum { get; private set; }

    public double Min { get; private set; } = double.PositiveInfinity;

    public double Max { get; private set; } = double.NegativeInfinity;

    public void Add(double value)
    {
        Count++;
        Sum += value;
        Min = Math.Min(Min, value);
        Max = Math.Max(Max, value);
    }

    public double Avg => Count == 0 ? 0 : Sum / Count;

    public bool Matches(AggregateResultMessage result, double tolerance = 0.000001)
    {
        return Count == result.Count
            && NearlyEqual(Sum, result.Sum, tolerance)
            && NearlyEqual(Min, result.Min, tolerance)
            && NearlyEqual(Max, result.Max, tolerance)
            && NearlyEqual(Avg, result.Avg, tolerance);
    }

    private static bool NearlyEqual(double left, double right, double tolerance)
        => Math.Abs(left - right) <= tolerance;
}
