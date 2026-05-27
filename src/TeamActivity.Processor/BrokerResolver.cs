using System.Diagnostics;

namespace TeamActivity.Processor;

/// <summary>
/// Resolves the MQTT broker's Docker container IP to bypass docker-proxy overhead.
/// Docker's userland proxy introduces ~75x throughput reduction for high-rate subscribers.
/// Falls back to the configured host if resolution fails.
/// </summary>
public static class BrokerResolver
{
    public static (string host, int port) Resolve(string configuredHost, int configuredPort)
    {
        if (configuredHost != "localhost" && configuredHost != "127.0.0.1")
            return (configuredHost, configuredPort);

        try
        {
            var containerIp = GetMosquittoContainerIp();
            if (containerIp is not null)
            {
                Console.WriteLine($"[BrokerResolver] Resolved container IP: {containerIp}:1883");
                return (containerIp, 1883);
            }
        }
        catch
        {
            // Fall through to configured endpoint
        }

        return (configuredHost, configuredPort);
    }

    private static string? GetMosquittoContainerIp()
    {
        var containerId = RunDocker(["ps", "-q", "--filter", "ancestor=eclipse-mosquitto:2.0"]);
        if (string.IsNullOrEmpty(containerId))
            return null;

        containerId = containerId.Split('\n')[0].Trim();
        if (string.IsNullOrEmpty(containerId))
            return null;

        var ip = RunDocker(["inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerId]);
        return !string.IsNullOrEmpty(ip) && ip.Contains('.') ? ip : null;
    }

    private static string RunDocker(string[] arguments)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "docker",
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };

        foreach (var arg in arguments)
            process.StartInfo.ArgumentList.Add(arg);

        process.Start();
        var output = process.StandardOutput.ReadToEnd().Trim();
        process.WaitForExit(3000);
        return output;
    }
}
