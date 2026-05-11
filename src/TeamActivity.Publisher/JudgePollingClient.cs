using System.Net.Http.Json;

namespace TeamActivity.Publisher;

public sealed class JudgePollingClient(HttpClient httpClient)
{
    public async Task<RunTriggerConfig?> GetPendingRun(CancellationToken cancellationToken)
    {
        try
        {
            var response = await httpClient.GetAsync("/api/run/pending", cancellationToken);
            if (response.StatusCode == System.Net.HttpStatusCode.NoContent) return null;
            return await response.Content.ReadFromJsonAsync<RunTriggerConfig>(cancellationToken);
        }
        catch
        {
            return null;
        }
    }

    public async Task<bool> IsRunActive(string runId, CancellationToken cancellationToken)
    {
        try
        {
            var response = await httpClient.GetAsync("/api/run/status", cancellationToken);
            if (!response.IsSuccessStatusCode) return false;
            var status = await response.Content.ReadFromJsonAsync<RunStatusSnapshot>(cancellationToken);
            return status?.Status == "Running" && status.Config?.RunId == runId;
        }
        catch
        {
            return false;
        }
    }

    public async Task AcknowledgeRun(string runId, CancellationToken cancellationToken)
    {
        try
        {
            await httpClient.PostAsJsonAsync("/api/run/acknowledge", new { runId }, cancellationToken);
        }
        catch
        {
            // Best-effort — Publisher will continue with the run regardless
        }
    }
}

public sealed record RunTriggerConfig(
    string RunId,
    int DeviceCount,
    int IntervalMs,
    int RunWindowSeconds,
    bool ChaosEnabled);

public sealed record RunStatusSnapshot(string Status, RunTriggerConfig? Config);
