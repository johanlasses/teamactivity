# MQTT AI Battle — Activity Rules

## What is this?

A 2-hour team engineering challenge. You improve a **.NET Aspire** telemetry pipeline — the team with the highest score wins.

A simulated **Publisher** fires sensor readings over MQTT. Your **Processor** consumes them, aggregates into 5-second windows, and publishes results back. An impartial **Judge** validates everything and keeps score in real time.

---

## The competition

When the 2 hours have passed we meet up and each team will run their solution with their choice of devices and interval.

- 2 minute run without chaos mode enabled
- 2 minute run **with** chaos mode enabled

Team with the most points will win. We will crown a non-chaos and chaos champion. 

---


## Your Task

Improve the **Publisher** and **Processor** to maximise your score. The template works end-to-end but is deliberately basic.

**What to improve:**
- Correct aggregate calculations for all devices and windows
- Publish results close to `windowEnd` (low latency)
- Handle MQTT reconnects and ensure no windows are dropped

**Do not modify:**
- `src/TeamActivity.Judge`
- `src/TeamActivity.Scoreboard`
- `src/TeamActivity.Shared.Contracts`
- `TeamActivity.AppHost`
- `scripts/`

Keep all MQTT topic patterns and JSON field names exactly as defined — any deviation results in `Invalid` or unscored messages.

---

## Scoring

**Maximum: 5000 points** — five categories, 1000 pts each.

| Category | 1000 pts (best) | 0 pts (worst) | When |
|---|---|---|---|
| **Interval** | 50 ms | 1000 ms | Fixed at run start |
| **Devices** | 50 000 devices | 1 device | Fixed at run start |
| **Publish Attainment** | 100% windows published | 0% published | Live |
| **Window Correctness** | 100% correct | 0% correct | Live |
| **Latency** | 100 ms P95 | ≥ 1000 ms P95 | Live |

**Interval** and **Devices** scores are locked in when you start the run. The other three depend entirely on your Processor.

> **Window scoring note:** The Judge only scores "fully observed" windows — where the actual broker message count matches the theoretical count. Missed Publisher messages reduce your Publish Attainment score.

---

## Starting a Run

Open the **Scoreboard** in your browser after starting Aspire — runs do **not** start automatically.

| Control | Default | Description |
|---|---|---|
| **Device Count** | `3` | Number of simulated devices. Higher = more load and more potential score. |
| **Message Interval (ms)** | `250` | Delay between messages per device. Lower = higher message rate. |
| **Run Window (seconds)** | `120` | Duration of the run. |
| **Enable Chaos Mode** | unchecked | Arms chaos disruptions for the run. |

Click **▶ Start Run**. The status badge goes: **Idle → Pending → Running → Idle**.

The `runId` is auto-generated as a movie character name for each run.

---

## Chaos Mode

Start a run with **Enable Chaos Mode** checked. The Scoreboard shows an amber banner when chaos is armed, and a red pulsing banner when an event fires. You know what can happen — not when.

| Event | What happens | What to handle |
|---|---|---|
| `processor-disconnect` | Your Processor is killed mid-run | Reconnect to MQTT, recover in-progress window state |
| `publisher-disconnect` | Your Publisher is killed mid-run | Reconnect to MQTT, recover in-progress window state |
| `message-duplications` | Duplicate messages injected | Deduplicate by `sequence` |

---
## Architecture

```
Publisher ──► MQTT broker ──► Processor ──► MQTT broker ──► Judge
                                                            │
                                               Scoreboard ◄─ HTTP /api/scores
```

| Service | Role |
|---|---|
| **Publisher** | Emits sensor readings and run lifecycle messages over MQTT |
| **Processor** | ⭐ **Your main work surface.** Aggregates readings into windows, publishes results |
| **Mosquitto** | MQTT broker — do not modify |
| **Judge** | Validates and scores results — do not modify |
| **Scoreboard** | Live leaderboard — do not modify |
| **Aspire Dashboard** | Logs, traces, and metrics for your pipeline |

---

## Setup

**Prerequisites:** [.NET 10 SDK](https://dotnet.microsoft.com/download) · [Docker Desktop](https://www.docker.com/products/docker-desktop/) · [Aspire CLI](https://aspire.dev/get-started/install-cli/)

```bash
git clone https://github.com/johanlasses/teamactivity.git
```

**Set your team ID** in `src/TeamActivity.Publisher/appsettings.json` so your results appear on the Scoreboard:

```json
"teamId": "your-team-name"
```

**Start:**
```bash
aspire start
```
The Aspire dashboard URL is printed in the terminal. Open it to inspect logs, traces, and metrics.

**Stop:**
```bash
aspire stop
```

---

## Reference: MQTT Topics

| Direction | Topic pattern | QoS |
|---|---|---|
| Publisher → broker | `telemetry/v1/{runId}/{teamId}/raw` | 0 |
| Publisher → broker | `control/v1/{runId}/{teamId}/publisher-start` | 1 |
| Publisher → broker | `control/v1/{runId}/{teamId}/publisher-complete` | 1 |
| Processor → broker | `results/v1/{runId}/{teamId}/device/{deviceId}/window/{windowStartUtcMs}` | 1 |

`{windowStartUtcMs}` is the window start as a Unix timestamp in **milliseconds** (UTC).

---

## Reference: Windowing Rules

Windows are **5-second, fixed, non-overlapping**, aligned to the Unix epoch.

```
windowStart = floor(eventTimeUtc_unix_ms / 5000) × 5000
windowEnd   = windowStart + 5000 ms
```

- Each device has its own independent set of windows
- Deduplicate readings by `runId|teamId|deviceId|sequence`
- **Grace period: 2 seconds** — results arriving after `windowEnd + 2s` are scored `Invalid`

---

## Reference: Message Schemas

All payloads are UTF-8 JSON with `"schemaVersion": 2`.

### TelemetryMessage (published by Publisher)

```json
{
  "schemaVersion": 2,
  "runId": "run-team-alpha",
  "teamId": "team-alpha",
  "deviceId": "device-001",
  "sequence": 1,
  "eventTimeUtc": "2025-01-01T10:00:00.000Z",
  "publishedAtUtc": "2025-01-01T10:00:00.001Z",
  "value": 42.1
}
```

### AggregateResultMessage (published by Processor)

```json
{
  "schemaVersion": 2,
  "runId": "run-team-alpha",
  "teamId": "team-alpha",
  "deviceId": "device-001",
  "windowStartUtc": "2025-01-01T10:00:00.000Z",
  "windowEndUtc": "2025-01-01T10:00:05.000Z",
  "count": 20,
  "sum": 842.0,
  "min": 41.1,
  "max": 43.1,
  "avg": 42.1,
  "resultId": "team-alpha-device-001-20250101T100000000Z",
  "publishedAtUtc": "2025-01-01T10:00:05.100Z"
}
```

`resultId` format: `{teamId}-{deviceId}-{windowStartUtc:yyyyMMddTHHmmssfffZ}`

### ControlMessage (published by Publisher)

```json
{
  "schemaVersion": 2,
  "runId": "run-team-alpha",
  "teamId": "team-alpha",
  "event": "publisher-start",
  "publishedAtUtc": "2025-01-01T10:00:00.000Z"
}
```

`event` is `publisher-start` or `publisher-complete`. **Do not remove the control message code from the Publisher** — the Scoreboard trigger depends on it.

---

## Agentic Help (Aspire MCP)

Connect your AI assistant to the Aspire MCP server for real-time introspection:

```bash
aspire agent init
```

Useful prompts: *"Are all resources running?"* · *"Show logs for the processor."* · *"List traces involving the scoreboard."*

---

## Guardrail Tests

```bash
bash scripts/smoke-test.sh
```

Builds, starts Aspire, triggers a short run, validates scoring, and stops Aspire.
