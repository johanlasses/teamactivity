# MQTT AI Battle — Operator Quick-Start

> **Participants:** see [Activity Rules.md](Activity%20Rules.md) for the full challenge rules, architecture overview, topic contracts, scoring formula, and task description.

This repository is the operator/developer reference for the **MQTT AI Battle** hackathon template built on .NET Aspire.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Aspire CLI](https://aspire.dev/get-started/install-cli/)

## Run

```bash
aspire start --apphost TeamActivity.AppHost/TeamActivity.AppHost.csproj
```

Open the Aspire dashboard URL printed in the terminal to inspect resource state, logs, traces, and metrics.

```bash
aspire stop --apphost TeamActivity.AppHost/TeamActivity.AppHost.csproj
```

## Guardrail tests

Validates the full pipeline end-to-end (build → run → score → Playwright UI):

```bash
bash scripts/smoke-test.sh
```

With the stack already running:

```bash
bash scripts/install-playwright.sh
RUN_SCOREBOARD_UI_TESTS=true SCOREBOARD_BASE_URL=http://localhost:5216 \
  dotnet test tests/TeamActivity.Scoreboard.Tests/TeamActivity.Scoreboard.Tests.csproj
```

## Aspire MCP (agentic assistance)

```bash
aspire agent init
```

Follow the prompts to connect an AI assistant to the Aspire MCP server. Do not commit the API key.

If automatic setup does not work:

1. Run the AppHost.
2. Open the Aspire dashboard and click **MCP**.
3. Copy the MCP URL, `type=http`, and `x-mcp-api-key` header into your assistant's MCP configuration.
