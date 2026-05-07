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
        if (!RunUiTests)
        {
            return;
        }

        playwright = await Playwright.CreateAsync();
        browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
        {
            Headless = true
        });
    }

    public async Task DisposeAsync()
    {
        if (browser is not null)
        {
            await browser.DisposeAsync();
        }

        playwright?.Dispose();
    }

    [Fact]
    public async Task ScoreboardDisplaysTheTemplateTeamScore()
    {
        if (!RunUiTests)
        {
            return;
        }

        Assert.NotNull(browser);

        var page = await browser.NewPageAsync();
        await page.GotoAsync(ScoreboardBaseUrl, new PageGotoOptions
        {
            WaitUntil = WaitUntilState.NetworkIdle
        });

        await ExpectText(page, "MQTT Telemetry Gauntlet");
        await page.WaitForFunctionAsync(
            "() => document.body.innerText.includes('team-template') && document.body.innerText.includes('run-template')",
            new PageWaitForFunctionOptions { Timeout = 15_000 });

        var content = await page.TextContentAsync("body") ?? string.Empty;
        Assert.Contains("Leaderboard", content);
        Assert.Contains("team-template", content);
        Assert.Contains("run-template", content);
        Assert.Contains("Observed messages", content);
    }

    [Fact]
    public async Task ScoreboardScoresApiReturnsTheJudgeSnapshot()
    {
        if (!RunUiTests)
        {
            return;
        }

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

    private static async Task ExpectText(IPage page, string expected)
    {
        await page.WaitForFunctionAsync(
            "expected => document.body.innerText.includes(expected)",
            expected,
            new PageWaitForFunctionOptions { Timeout = 10_000 });
    }
}
