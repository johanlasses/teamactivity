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

    // ── Existing tests (updated) ──────────────────────────────────────────────

    [Fact]
    public async Task ScoreboardDisplaysTheTemplateTeamScore()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await ExpectText(page, "MQTT AI Battle");
        // RunId is now a UUID — only check for team identity
        await page.WaitForFunctionAsync(
            "() => document.body.innerText.includes('team-template')",
            new PageWaitForFunctionOptions { Timeout = 15_000 });

        var content = await page.TextContentAsync("body") ?? string.Empty;
        Assert.Contains("Leaderboard", content);
        Assert.Contains("team-template", content);
        Assert.Contains("Observed messages", content);
    }

    [Fact]
    public async Task ScoreboardScoresApiReturnsTheJudgeSnapshot()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        var response = await page.GotoAsync($"{ScoreboardBaseUrl}/api/scores");

        Assert.NotNull(response);
        Assert.True(response.Ok, $"Expected /api/scores to return success but got {response.Status}.");

        var body = await page.TextContentAsync("body") ?? string.Empty;
        Assert.Contains("team-template", body);
        Assert.Contains("\"correct\":", body);
        Assert.DoesNotContain("\"missing\":1", body);
    }

    // ── Control panel presence and defaults ───────────────────────────────────

    [Fact]
    public async Task ScoreboardShowsControlPanel()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        Assert.NotNull(await page.QuerySelectorAsync("#device-count"));
        Assert.NotNull(await page.QuerySelectorAsync("#message-interval"));
        Assert.NotNull(await page.QuerySelectorAsync("#run-window"));
        Assert.NotNull(await page.QuerySelectorAsync("#chaos-mode"));
        Assert.NotNull(await page.QuerySelectorAsync("#start-btn"));
        Assert.NotNull(await page.QuerySelectorAsync("#run-status-badge"));
    }

    [Fact]
    public async Task ScoreboardInputDefaultsAreReasonable()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        Assert.Equal("3", await page.InputValueAsync("#device-count"));
        Assert.Equal("250", await page.InputValueAsync("#message-interval"));
        Assert.Equal("120", await page.InputValueAsync("#run-window"));
        Assert.False(await page.IsCheckedAsync("#chaos-mode"));
    }

    [Fact]
    public async Task ScoreboardRunStatusStartsIdle()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        var text = await page.TextContentAsync("#run-status-badge") ?? string.Empty;
        Assert.Contains("Idle", text, StringComparison.OrdinalIgnoreCase);
    }

    // ── Start button interaction ──────────────────────────────────────────────

    [Fact]
    public async Task ScoreboardStartButtonTriggersRun()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await page.FillAsync("#run-window", "8");
        await page.ClickAsync("#start-btn");

        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-feedback')?.textContent?.includes('Run started')",
            new PageWaitForFunctionOptions { Timeout = 10_000 });

        await page.WaitForFunctionAsync(
            "() => document.body.innerText.includes('team-template')",
            new PageWaitForFunctionOptions { Timeout = 25_000 });

        var content = await page.TextContentAsync("body") ?? string.Empty;
        Assert.Contains("team-template", content);
    }

    [Fact]
    public async Task ScoreboardStartButtonDisabledWhileRunning()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await page.FillAsync("#run-window", "30");
        await page.ClickAsync("#start-btn");

        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-btn')?.disabled === true",
            new PageWaitForFunctionOptions { Timeout = 5_000 });

        Assert.True(await page.IsDisabledAsync("#start-btn"));
    }

    [Fact]
    public async Task ScoreboardRunStatusUpdatesToRunning()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await page.FillAsync("#run-window", "30");
        await page.ClickAsync("#start-btn");

        await page.WaitForFunctionAsync(
            "() => { const t = document.getElementById('run-status-badge')?.textContent?.toLowerCase(); return t === 'pending' || t === 'running'; }",
            new PageWaitForFunctionOptions { Timeout = 10_000 });

        var badgeText = await page.TextContentAsync("#run-status-badge") ?? string.Empty;
        Assert.True(
            badgeText.Contains("Pending", StringComparison.OrdinalIgnoreCase) ||
            badgeText.Contains("Running", StringComparison.OrdinalIgnoreCase));
    }

    // ── Chaos mode ────────────────────────────────────────────────────────────

    [Fact]
    public async Task ScoreboardChaosCheckboxEnablesChaos()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await page.CheckAsync("#chaos-mode");
        await page.FillAsync("#run-window", "8");
        await page.ClickAsync("#start-btn");

        await page.WaitForFunctionAsync(
            "() => { const c = document.getElementById('chaos-banner')?.className; return c?.includes('armed') || c?.includes('active'); }",
            new PageWaitForFunctionOptions { Timeout = 15_000 });

        var bannerClass = await page.GetAttributeAsync("#chaos-banner", "class") ?? string.Empty;
        Assert.True(bannerClass is "armed" or "active", $"Expected armed or active chaos banner, got '{bannerClass}'");
    }

    [Fact]
    public async Task ScoreboardChaosUncheckedNoChaosBanner()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await page.UncheckAsync("#chaos-mode");
        await page.FillAsync("#run-window", "8");
        await page.ClickAsync("#start-btn");

        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-feedback')?.textContent?.includes('Run started')",
            new PageWaitForFunctionOptions { Timeout = 10_000 });

        var bannerClass = await page.GetAttributeAsync("#chaos-banner", "class") ?? string.Empty;
        Assert.True(string.IsNullOrEmpty(bannerClass), $"Expected no chaos banner but got class '{bannerClass}'");
    }

    // ── Input validation ──────────────────────────────────────────────────────

    [Fact]
    public async Task ScoreboardInvalidIntervalPreventsStart()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await page.FillAsync("#message-interval", "0");
        await page.ClickAsync("#start-btn");

        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-feedback')?.textContent?.length > 0",
            new PageWaitForFunctionOptions { Timeout = 5_000 });

        var feedback = await page.TextContentAsync("#start-feedback") ?? string.Empty;
        Assert.Contains("Interval", feedback, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ScoreboardInvalidDeviceCountPreventsStart()
    {
        if (!RunUiTests) return;
        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        await page.FillAsync("#device-count", "0");
        await page.ClickAsync("#start-btn");

        await page.WaitForFunctionAsync(
            "() => document.getElementById('start-feedback')?.textContent?.length > 0",
            new PageWaitForFunctionOptions { Timeout = 5_000 });

        var feedback = await page.TextContentAsync("#start-feedback") ?? string.Empty;
        Assert.Contains("Device", feedback, StringComparison.OrdinalIgnoreCase);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task ExpectText(IPage page, string expected)
    {
        await page.WaitForFunctionAsync(
            "expected => document.body.innerText.includes(expected)",
            expected,
            new PageWaitForFunctionOptions { Timeout = 10_000 });
    }
}
