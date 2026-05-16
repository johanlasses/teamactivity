using Microsoft.Playwright;

namespace TeamActivity.Scoreboard.Tests;

public sealed class ScoreboardUiTests : IAsyncLifetime
{
    private IPlaywright? playwright;
    private IBrowser? browser;

    private static bool RunUiTests =>
        string.Equals(Environment.GetEnvironmentVariable("RUN_SCOREBOARD_UI_TESTS"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ScoreboardBaseUrl =>
        Environment.GetEnvironmentVariable("SCOREBOARD_BASE_URL") ?? "http://localhost:5216";

    public async Task InitializeAsync()
    {
        if (!RunUiTests) return;
        playwright = await Playwright.CreateAsync();
        browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions { Headless = true });
    }

    public async Task DisposeAsync()
    {
        if (browser is not null) await browser.DisposeAsync();
        playwright?.Dispose();
    }

    // ── Scenario 1: No chaos ──────────────────────────────────────────────────

    /// <summary>
    /// Starts a 10-second run with chaos disabled.
    /// Verifies the run completes, the processor handled messages, and the result
    /// is visible in the leaderboard with at least one correct message.
    /// </summary>
    [Fact]
    public async Task RunWithoutChaosCompletesAndShowsScore()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        await EnsureIdleAsync();

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        // Confirm we start from idle
        var initialStatus = await page.TextContentAsync("#run-status-badge") ?? string.Empty;
        Assert.Contains("Idle", initialStatus, StringComparison.OrdinalIgnoreCase);

        // Chaos must be off
        await page.UncheckAsync("#chaos-mode");

        // Set a short run window
        await page.FillAsync("#run-window", "10");
        await page.ClickAsync("#start-btn");

        // Confirm run started ("Run started: <Name>" — name is random, just check the prefix)
        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-feedback')?.textContent?.includes('Run started:')",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        var feedback = await page.TextContentAsync("#start-feedback") ?? string.Empty;
        Assert.StartsWith("Run started:", feedback.Trim(), StringComparison.OrdinalIgnoreCase);

        // Wait for the run to reach Running state
        await page.WaitForFunctionAsync(
            "() => { const t = document.getElementById('run-status-badge')?.textContent?.toLowerCase(); return t?.includes('running') || t?.includes('pending'); }",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        // Wait for the run to complete (10 s window + finalization grace)
        await page.WaitForFunctionAsync(
            "() => document.getElementById('run-status-badge')?.textContent?.toLowerCase().includes('idle')",
            null, new PageWaitForFunctionOptions { Timeout = 30_000 });

        // No chaos banner should be visible
        var bannerClass = await page.GetAttributeAsync("#chaos-banner", "class") ?? string.Empty;
        Assert.True(string.IsNullOrEmpty(bannerClass), $"Expected no chaos banner but got class '{bannerClass}'");

        // Wait an extra polling cycle (2 s) to ensure the table is refreshed
        await Task.Delay(3_000);

        // Leaderboard must show a row for this run with correct > 0
        var correctValue = await page.EvaluateAsync<int>(
            """
            (() => {
              const rows = document.querySelectorAll('#scores tr');
              for (const row of rows) {
                const cells = row.querySelectorAll('td');
                if (cells.length < 4) continue;
                if (!row.textContent.includes('team-template')) continue;
                const v = parseInt(cells[3].textContent ?? '0', 10);
                if (v > 0) return v;
              }
              return -1;
            })()
            """);

        var tableSnapshot = await page.EvaluateAsync<string[]>(
            "Array.from(document.querySelectorAll('#scores tr')).slice(0,5).map(r => r.textContent.trim().replace(/\\s+/g,' '))");

        Assert.True(correctValue > 0,
            $"Expected a team-template row with correct > 0 but got {correctValue}. " +
            $"Table snapshot: [{string.Join(" | ", tableSnapshot)}]");
    }

    // ── Scenario 2: Chaos enabled ─────────────────────────────────────────────

    /// <summary>
    /// Starts a 30-second run with chaos enabled.
    /// Verifies: chaos banner shows "armed" on start, live scoring updates appear
    /// during the run, a chaos event fired via the organizer panel shows as "active"
    /// in the banner, and the run completes with a score.
    /// </summary>
    [Fact]
    public async Task RunWithChaosShowsLiveScoringAndChaosEvent()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        await EnsureIdleAsync();

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        // Enable chaos and set run window
        await page.CheckAsync("#chaos-mode");
        await page.FillAsync("#run-window", "30");
        await page.ClickAsync("#start-btn");

        // Confirm run started ("Run started: <Name>" — name is random, just check the prefix)
        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-feedback')?.textContent?.includes('Run started:')",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        var feedback = await page.TextContentAsync("#start-feedback") ?? string.Empty;
        Assert.StartsWith("Run started:", feedback.Trim(), StringComparison.OrdinalIgnoreCase);

        // Run must reach Running state
        await page.WaitForFunctionAsync(
            "() => document.getElementById('run-status-badge')?.textContent?.toLowerCase().includes('running')",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        // Chaos banner must show "armed" — chaos mode is active but no event yet
        await page.WaitForFunctionAsync(
            "() => document.getElementById('chaos-banner')?.className === 'armed'",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        var armedClass = await page.GetAttributeAsync("#chaos-banner", "class");
        Assert.Equal("armed", armedClass);

        // Wait for the first scoring window to finalise (~7 s: 5 s window + 2 s grace)
        // then verify a score row appears — proves live scoring works
        await page.WaitForFunctionAsync(
            """
            () => {
              const rows = document.querySelectorAll('#scores tr');
              return Array.from(rows).some(row => {
                if (!row.textContent.includes('team-template')) return false;
                const correct = parseInt(row.querySelectorAll('td')[3]?.textContent ?? '0', 10);
                return correct > 0;
              });
            }
            """,
            null, new PageWaitForFunctionOptions { Timeout = 20_000 });

        // Fire a chaos event via the organizer panel
        await page.WaitForFunctionAsync(
            "() => document.getElementById('organizer-panel')?.style.display !== 'none'",
            null, new PageWaitForFunctionOptions { Timeout = 3_000 });

        // Click the first chaos event button (Message Gap)
        var chaosBtns = await page.QuerySelectorAllAsync(".chaos-btn");
        Assert.True(chaosBtns.Count > 0, "Expected at least one chaos event button");
        await chaosBtns[0].ClickAsync();

        // Banner must transition to "active" — chaos event is now shown in the UI
        await page.WaitForFunctionAsync(
            "() => document.getElementById('chaos-banner')?.className === 'active'",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        var activeClass = await page.GetAttributeAsync("#chaos-banner", "class");
        Assert.Equal("active", activeClass);

        // End the chaos event and verify the banner returns to "armed"
        await page.ClickAsync("#end-event-btn");
        await page.WaitForFunctionAsync(
            "() => document.getElementById('chaos-banner')?.className === 'armed'",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        var postEventClass = await page.GetAttributeAsync("#chaos-banner", "class");
        Assert.Equal("armed", postEventClass);

        // Wait for the full run to complete
        await page.WaitForFunctionAsync(
            "() => document.getElementById('run-status-badge')?.textContent?.toLowerCase().includes('idle')",
            null, new PageWaitForFunctionOptions { Timeout = 45_000 });

        // Final leaderboard must still show a team-template row with correct > 0
        await Task.Delay(3_000);
        var finalCorrect = await page.EvaluateAsync<int>(
            """
            (() => {
              const rows = document.querySelectorAll('#scores tr');
              for (const row of rows) {
                const cells = row.querySelectorAll('td');
                if (cells.length < 4) continue;
                if (!row.textContent.includes('team-template')) continue;
                const v = parseInt(cells[3].textContent ?? '0', 10);
                if (v > 0) return v;
              }
              return -1;
            })()
            """);
        Assert.True(finalCorrect > 0,
            $"Expected a team-template row with correct > 0 in final leaderboard but got {finalCorrect}");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>Stops any active run and waits for the scoreboard to reflect idle.</summary>
    private async Task EnsureIdleAsync()
    {
        using var http = new HttpClient();
        await http.PostAsync($"{ScoreboardBaseUrl}/api/run/stop", null);
        await Task.Delay(2_500);
    }
}
