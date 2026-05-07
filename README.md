# MQTT Telemetry Gauntlet

This repository is a .NET Aspire starter template for the hackathon challenge.

Phase 1 is intentionally small:

- Aspire AppHost
- Mosquitto MQTT broker
- Publisher starter service
- Processor starter service
- Judge service
- Standalone Scoreboard web app
- OpenTelemetry logs/traces/metrics through Aspire ServiceDefaults

The Publisher and Processor are deliberately basic. They use raw JSON and MQTT only. Do not treat them as production-ready clients; improving them is part of the challenge.

## Run locally

You need to install the aspire cli (https://aspire.dev/get-started/install-cli/)
```bash
aspire start/stop
```

Open the Aspire dashboard from the URL printed by the AppHost. The dashboard shows resource state, logs, traces, and metrics.

## Guardrail tests

The solution includes Playwright tests for the Scoreboard. They assume the Aspire stack is already running and that the Judge has produced a score.

Run the full smoke flow:

```bash
bash scripts/smoke-test.sh
```

The script starts Aspire with `aspire start`, waits for the template run to score, installs the Chromium browser for Playwright if needed, runs the Scoreboard tests, and stops Aspire with `aspire stop`.

If you already have the stack running:

```bash
bash scripts/install-playwright.sh
RUN_SCOREBOARD_UI_TESTS=true SCOREBOARD_BASE_URL=http://localhost:5216 dotnet test tests/TeamActivity.Scoreboard.Tests/TeamActivity.Scoreboard.Tests.csproj
```

## Starter flow

The default run uses:

- `runId`: `run-template`
- `teamId`: `team-template`
- telemetry topic: `telemetry/v1/run-template/team-template/raw`
- control topics:
  - `control/v1/run-template/team-template/publisher-start`
  - `control/v1/run-template/team-template/publisher-complete`

The Publisher emits one telemetry JSON message. The Processor subscribes to the telemetry topic and logs the message it receives. The Judge subscribes to telemetry and control topics, validates basic topic/payload alignment, and exposes observed runs to the Scoreboard.

## Scoreboard

The Scoreboard is a standalone ASP.NET Core web app.

It reads the Judge HTTP API and displays observed runs, messages, and scores. Runtime diagnostics live in the Aspire dashboard through OpenTelemetry logs, traces, and metrics.

## Agentic harnessing with Aspire MCP

Aspire includes an MCP server for local development agents. Use it so Copilot or another assistant can inspect resources, logs, traces, and health without guessing.

From the Aspire project directory, run:

```bash
aspire agent init
```

Follow the prompts to configure your AI assistant.

If automatic setup does not work:

1. Run the AppHost.
2. Open the Aspire dashboard.
3. Click the **MCP** button.
4. Copy the MCP URL, `type=http`, and `x-mcp-api-key` header into your assistant's MCP configuration.
5. Store the API key securely. Do not commit it.

Useful Copilot CLI prompts after MCP is configured:

- Are all Aspire resources running?
- Show console logs for the processor.
- Show structured logs for the judge.
- List traces involving the scoreboard.

## What teams should change

Start in:

- `src/TeamActivity.Publisher`
- `src/TeamActivity.Processor`

Keep the MQTT topic and JSON contracts aligned with `src/TeamActivity.Shared.Contracts`.

Scoring is implemented in the Judge. Keep the Judge as the source of truth; do not recompute scores in the Scoreboard or team services.
