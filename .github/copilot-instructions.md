# Copilot instructions for MQTT Telemetry Gauntlet

This repository is a .NET Aspire hackathon template.

## Architecture rules

- The Aspire AppHost orchestrates everything.
- Mosquitto is the MQTT broker.
- Publisher and Processor are starter C#/.NET Worker Services.
- Judge is the source of truth for validation and scoring.
- Scoreboard is a standalone ASP.NET Core web app that reads Judge data.
- Aspire dashboard is the observability surface through OpenTelemetry logs, traces, and metrics.
- Do not add Grafana, Prometheus, or a database unless the user explicitly changes scope again.

## Phase 1 constraints

- Keep Publisher and Processor deliberately basic.
- Use raw JSON payloads only.
- Do not add compression.
- Do not add custom Polly policies, retries, circuit breakers, or backoff logic.
- Standard Aspire ServiceDefaults are allowed and expected.
- Full scoring lives in the Judge.

## Aspire MCP workflow

Use Aspire MCP whenever the AppHost is running. It is the best way to inspect resources, logs, traces, and health.

Recommended setup:

```bash
aspire agent init
```

If automatic setup does not work:

1. Run the AppHost with `aspire start --apphost TeamActivity.AppHost/TeamActivity.AppHost.csproj`.
2. Open the Aspire dashboard.
3. Click the MCP button.
4. Configure the assistant with the displayed MCP URL, `type=http`, and `x-mcp-api-key` header.
5. Do not commit MCP API keys.

Useful prompts:

- Are all Aspire resources running?
- Show console logs for the processor.
- Show structured logs for the judge.
- List traces involving the scoreboard.

## Running the stack

Prefer Aspire CLI lifecycle commands:

```bash
aspire start --apphost TeamActivity.AppHost/TeamActivity.AppHost.csproj
aspire stop --apphost TeamActivity.AppHost/TeamActivity.AppHost.csproj
```

Do not leave orphaned `dotnet run` processes or containers.

## Guardrail tests

Use the Playwright Scoreboard tests before and after changing the Publisher, Processor, Judge, or Scoreboard:

```bash
bash scripts/smoke-test.sh
```

The tests assume the Scoreboard is available and the Judge has scored at least one template run.
