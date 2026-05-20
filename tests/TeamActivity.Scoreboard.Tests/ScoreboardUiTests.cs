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

        // Set known run parameters so scoring assertions are deterministic
        await page.FillAsync("#device-count", "3");
        await page.FillAsync("#message-interval", "250");
        await page.FillAsync("#run-window", "10");
        await page.ClickAsync("#start-btn");

        // Confirm run started ("Run started: <Name>" — name is random, just check the prefix)
        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-feedback')?.textContent?.includes('Run started:')",
            null, new PageWaitForFunctionOptions { Timeout = 10_000 });

        var feedback = await page.TextContentAsync("#start-feedback") ?? string.Empty;
        Assert.StartsWith("Run started:", feedback.Trim(), StringComparison.OrdinalIgnoreCase);

        // Capture the run name so we can target this specific row later (avoids matching stale rows)
        var runName = feedback.Trim()["Run started:".Length..].Trim();

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

        // ── Scoring 2.0 widget assertions ─────────────────────────────────────

        // Score column must show "/ 5000" (Scoring 2.0 max) — pass runName as a JS argument
        var scoreText = await page.EvaluateAsync<string>(
            """
            (name) => {
                const rows = document.querySelectorAll('#scores tr.expand-row');
                for (const row of rows) {
                    if (row.textContent.includes(name)) {
                        return row.querySelectorAll('td')[2]?.textContent ?? '';
                    }
                }
                return '';
            }
            """, runName);
        Assert.Contains("/ 5000", scoreText, StringComparison.Ordinal);

        // Expand the score breakdown widget by clicking the row
        await page.EvaluateAsync(
            """
            (name) => {
                const rows = document.querySelectorAll('#scores tr.expand-row');
                for (const row of rows) {
                    if (row.textContent.includes(name)) { row.click(); return; }
                }
            }
            """, runName);

        // Wait for the detail row to become visible with all 5 bars
        await page.WaitForFunctionAsync(
            """
            () => {
                const details = document.querySelectorAll('#scores tr.score-detail-row');
                for (const detail of details) {
                    if (detail.style.display === 'none') continue;
                    if (detail.querySelectorAll('.bar-value').length === 5) return true;
                }
                return false;
            }
            """,
            null, new PageWaitForFunctionOptions { Timeout = 5_000 });

        // Read bar values in order: Interval, Devices, Publish Attainment, Window Correctness, Latency
        var barValues = await page.EvaluateAsync<string[]>(
            """
            (() => {
                const details = document.querySelectorAll('#scores tr.score-detail-row');
                for (const detail of details) {
                    if (detail.style.display === 'none') continue;
                    const vals = Array.from(detail.querySelectorAll('.bar-value')).map(el => el.textContent.trim());
                    if (vals.length === 5) return vals;
                }
                return [];
            })()
            """);

        Assert.True(barValues.Length == 5,
            $"Expected 5 score breakdown bars but got {barValues.Length}. Values: [{string.Join(", ", barValues)}]");

        // Interval bar: 250 ms → 1000 × (1000 − 250) / (1000 − 50) ≈ 789 pts (fixed at run start)
        var intervalBarScore = int.Parse(barValues[0]);
        Assert.InRange(intervalBarScore, 788, 791);

        // Device bar: 3 devices → 1000 × 3 / 50 000 ≈ 0 pts (fixed at run start)
        var deviceBarScore = int.Parse(barValues[1]);
        Assert.InRange(deviceBarScore, 0, 1);
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
