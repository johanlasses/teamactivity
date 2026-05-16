# MQTT AI Battle — Activity Rules

## Event Overview

Welcome to **MQTT AI Battle** — a hands-on team engineering challenge where you race to build the most accurate and low-latency MQTT telemetry pipeline.

Each team starts from the same template project. A simulated Publisher fires sensor readings at high frequency across multiple virtual devices. Your job is to consume those readings, aggregate them into fixed-size time windows, and publish the results back over MQTT — fast and accurately. An impartial **Judge** service validates everything and keeps score in real time. The team with the highest score wins.

---

## Template Project — Architecture

The template is a **.NET Aspire** solution. Aspire orchestrates all services and provides a built-in observability dashboard.

```
Publisher ──► MQTT broker (Mosquitto) ──► Processor ──► MQTT broker ──► Judge
                                                                        │
                                                             Scoreboard ◄─ HTTP /api/scores
```

| Service | Role |
|---|---|
| **Mosquitto** | MQTT broker — the message bus every service connects to (port 1883) |
| **Publisher** | Simulates sensor devices; emits `TelemetryMessage` JSON and lifecycle `ControlMessage` JSON over MQTT |
| **Processor** | **Your main work surface.** Subscribes to telemetry, aggregates readings into time windows, and publishes `AggregateResultMessage` JSON back to MQTT |
| **Judge** | Impartial validator and scorer. Subscribes to all topics, computes expected aggregates independently, and scores each result window |
| **Scoreboard** | ASP.NET Core web app that reads the Judge HTTP API and displays the live leaderboard |
| **Aspire Dashboard** | Observability surface for logs, traces, and metrics via OpenTelemetry |

---

## How to Run the Stack

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (for the Mosquitto container)
- [Aspire CLI](https://aspire.dev/get-started/install-cli/)

### Start

```bash
aspire start --apphost TeamActivity.AppHost/TeamActivity.AppHost.csproj
```

The Aspire dashboard URL is printed in the terminal. Open it to view resource state, logs, traces, and metrics.

### Stop

```bash
aspire stop --apphost TeamActivity.AppHost/TeamActivity.AppHost.csproj
```

> **Do not** leave orphaned `dotnet run` processes or containers running between sessions.

---

## Running the Guardrail Tests

A smoke-test script validates the full pipeline from start to score:

```bash
bash scripts/smoke-test.sh
```

The script:
1. Builds the solution
2. Starts Aspire
3. Triggers a short run via the Judge API
4. Waits for the run to produce scored windows
5. Checks the Judge API for at least one correct aggregate
6. Installs the Playwright Chromium browser if needed
7. Runs the Scoreboard Playwright UI tests
8. Stops Aspire

If the stack is already running you can run just the UI tests:

```bash
bash scripts/install-playwright.sh
RUN_SCOREBOARD_UI_TESTS=true SCOREBOARD_BASE_URL=http://localhost:5216 \
  dotnet test tests/TeamActivity.Scoreboard.Tests/TeamActivity.Scoreboard.Tests.csproj
```

---

## MQTT Topic Contracts

All topics follow a versioned hierarchy using `runId` and `teamId` to namespace each team's traffic.

| Direction | Topic pattern | QoS |
|---|---|---|
| Publisher → broker | `telemetry/v1/{runId}/{teamId}/raw` | 0 (at most once) |
| Publisher → broker | `control/v1/{runId}/{teamId}/publisher-start` | 1 (at least once) |
| Publisher → broker | `control/v1/{runId}/{teamId}/publisher-complete` | 1 (at least once) |
| Processor → broker | `results/v1/{runId}/{teamId}/device/{deviceId}/window/{windowStartUtcMs}` | 1 (at least once) |

`{windowStartUtcMs}` is the Unix timestamp in **milliseconds** of the window start (UTC).

---

## Message Schemas (JSON)

All payloads are UTF-8 JSON. **`schemaVersion` must always be `2`.**

### TelemetryMessage

Published by the Publisher once per reading.

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

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | int | Must be `2` |
| `runId` | string | Identifies the run |
| `teamId` | string | Identifies the team |
| `deviceId` | string | Identifies the simulated device (e.g. `device-001`) |
| `sequence` | long | Monotonically increasing per Publisher run |
| `eventTimeUtc` | ISO 8601 | When the reading was taken — used for window assignment |
| `publishedAtUtc` | ISO 8601 | When the message was published |
| `value` | double | The sensor reading |

### AggregateResultMessage

Published by the Processor once per completed window, per device.

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

| Field | Type | Description |
|---|---|---|
| `schemaVersion` | int | Must be `2` |
| `runId` | string | Must match the telemetry |
| `teamId` | string | Must match the telemetry |
| `deviceId` | string | Must match the telemetry |
| `windowStartUtc` | ISO 8601 | Start of the aggregation window (inclusive) |
| `windowEndUtc` | ISO 8601 | End of the aggregation window (exclusive) |
| `count` | int | Number of readings in the window |
| `sum` | double | Sum of all `value` readings |
| `min` | double | Minimum `value` |
| `max` | double | Maximum `value` |
| `avg` | double | Arithmetic mean (`sum / count`) |
| `resultId` | string | Unique identifier for this result (format: `{teamId}-{deviceId}-{windowStartUtc:yyyyMMddTHHmmssfffZ}`) |
| `publishedAtUtc` | ISO 8601 | When the result was published |

### ControlMessage

Published by the Publisher at the start and end of a run.

```json
{
  "schemaVersion": 2,
  "runId": "run-team-alpha",
  "teamId": "team-alpha",
  "event": "publisher-start",
  "publishedAtUtc": "2025-01-01T10:00:00.000Z"
}
```

`event` is either `publisher-start` or `publisher-complete`.

---

## Windowing Rules

The Processor must aggregate telemetry readings into **fixed, non-overlapping 5-second windows** aligned to the Unix epoch (UTC).

**Window assignment:** a reading with `eventTimeUtc = T` belongs to the window that starts at:

```
windowStart = floor(T_unix_ms / 5000) × 5000   (in milliseconds)
windowEnd   = windowStart + 5000 ms
```

**Per device:** each device gets its own independent set of windows. A result covers exactly one device and one window.

**Deduplication:** readings are deduplicated by `runId|teamId|deviceId|sequence` — each unique combination is counted at most once.

**Grace period:** the Judge finalises a window **2 seconds** after `windowEnd`. Results that arrive after that are scored as `Invalid`.

---

## Scoring

The Judge combines **four main criteria** — interval challenge, device challenge, publisher attainment, and processor correctness — once windows have cleared the grace period.

```
Score =
  1000 × IntervalChallenge
  + 1000 × DeviceChallenge
  + 1000 × PublishAttainment
  + 1000 × WindowCorrectness
  − 1200 × WindowInvalidRate
  − 800 × WindowMissingRate
  + 10 × log10(1 + Correct)
  − 0.05 × LatencyP95ms
```

| Term | Definition |
|---|---|
| **IntervalChallenge** | `min(1, 50 / intervalMs)` so `50 ms` gets full points and higher intervals scale down |
| **DeviceChallenge** | `min(1, deviceCount / 10000)` so `10,000` devices gets full points and smaller runs scale down |
| **PublishAttainment** | `broker-observed telemetry / theoretical telemetry`, capped at `1.0` |
| **WindowCorrectness** | `correct windows / fully observed windows` |
| **WindowInvalidRate** | `invalid windows / fully observed windows` |
| **WindowMissingRate** | `missing windows / fully observed windows` |
| **Correct volume bonus** | `10 × log10(1 + Correct)` so each 10× increase in correct windows adds a modest bonus |
| **LatencyP95ms** | 95th-percentile of `(result received at Judge − windowEnd)` across all correct results (in milliseconds) |

### Window validation rules

- The Judge computes a **theoretical count per device/window** from the configured run start, run duration, device count, and message interval.
- The Judge also counts how many telemetry messages actually reached the broker for each device/window.
- A window is **fully observed** only when the broker-observed count matches the theoretical count.
- Processor results are scored as **Correct**, **Invalid**, or **Missing** only for fully observed windows.
- Windows where the publisher did not hit the theoretical count are tracked separately as **publisher mismatch windows** and affect total score through publish attainment instead of being double-penalised as processor failures.
- Interval and device challenge scores are each **capped at 1.0**, so teams cannot gain extra points beyond the `50 ms` and `10,000 device` baselines.

---

## The Task

Your goal is to maximise the score produced by the Judge for your team's run.

### Starting point

The template Publisher and Processor are deliberately minimal. They work end-to-end but leave significant room for improvement:

- `src/TeamActivity.Publisher` — emits telemetry and lifecycle control messages
- `src/TeamActivity.Processor` — subscribes to telemetry, aggregates into windows, publishes results

### What you should improve

- Reduce result latency (publish results as close to `windowEnd` as possible)
- Improve reliability (handle MQTT reconnects, ensure all windows are published)
- Ensure correct aggregate calculations for all devices and windows
- Use the Aspire dashboard logs, traces, and metrics to observe and debug your pipeline

### What you must not change

The following are the event's invariants — **do not modify** them:

- `src/TeamActivity.Judge` — the impartial scorer
- `src/TeamActivity.Scoreboard` — the leaderboard UI
- `src/TeamActivity.Shared.Contracts` — MQTT topic patterns and JSON message schemas
- `TeamActivity.AppHost` — the Aspire orchestration config
- `scripts/` — the guardrail test scripts

Keep the MQTT topic patterns and JSON field names exactly as defined above. Any deviation will result in `Invalid` or unscored messages.

---

## Scoreboard Control Panel

The Scoreboard is both a leaderboard and an **interactive run launcher**. Open it in a browser after starting Aspire — runs do **not** start automatically.

### Starting a Run

At the top of the Scoreboard page is the **Start a Run** control panel:

| Control | Default | Description |
|---|---|---|
| **Device Count** | `3` | Number of simulated devices (`device-001` … `device-N`). Higher = more parallelism and more messages. |
| **Message Interval (ms)** | `250` | Milliseconds between successive messages from each device. Lower = higher message rate. |
| **Run Window (seconds)** | `120` | How long the Publisher emits messages. The 2-minute competitive window = 120. |
| **Enable Chaos Mode** | unchecked | When checked, chaos disruptions are armed as soon as the run begins. |
| **▶ Start Run** button | — | Sends the trigger to the Judge. The Publisher immediately begins emitting telemetry. |

The **Run Status** badge updates every 2 seconds: **Idle** → **Pending** (trigger received, waiting for Publisher to acknowledge) → **Running** → **Idle** (run complete).

The Start button and all inputs are **disabled** while a run is in progress. Multiple concurrent runs are not supported.

### Starting a Run via the API (scripts)

The same trigger endpoint is available directly on the Judge service:

```bash
curl -X POST http://localhost:5076/api/run/start \
  -H 'Content-Type: application/json' \
  -d '{"deviceCount":3,"intervalMs":250,"runWindowSeconds":120,"chaosEnabled":false}'
```

The response is `200 OK` with `{"runId":"<uuid>"}`, or `409 Conflict` if a run is already in progress.

---

## Default Run Parameters

The Publisher reads identity from its `appsettings.json`:

| Parameter | Default | Notes |
|---|---|---|
| `teamId` | `team-template` | Set via `ChallengeOptions` — identifies your team on the Scoreboard |
| `WindowSeconds` | `5` | Aggregation window size (fixed) |
| `GraceSeconds` | `2` | Grace period before Judge finalises a window (fixed) |
| `StartupDelaySeconds` | `3` | Publisher waits for Judge and Processor to be ready before polling for a trigger |

**Run parameters are set per-run via the trigger** (Scoreboard UI or API), not in the Publisher config:

| Parameter | Trigger field | Notes |
|---|---|---|
| Device Count | `deviceCount` | Simulated devices (`device-001` … `device-N`) |
| Message Interval | `intervalMs` | Delay between messages in milliseconds |
| Run Window | `runWindowSeconds` | Duration the Publisher emits telemetry |
| Chaos Mode | `chaosEnabled` | Arms chaos disruptions for the run |

**Message count is derived automatically per device** from `runWindowSeconds` and `intervalMs`. The theoretical total is:

```text
messagesPerDevice = ceil(runWindowSeconds × 1000 / intervalMs)
theoreticalTotalMessages = deviceCount × messagesPerDevice
```

For example with the defaults (120 s window, 250 ms interval):
- **3 devices × 480 messages** = 1 440 total messages
- `100 ms` interval → 3 × 1 200 = **3 600 messages**
- `50 ms` interval → 3 × 2 400 = **7 200 messages**

**Each team must set their own `teamId`** in `Publisher/appsettings.json` (or via environment variable) so their results appear separately on the Scoreboard. The `runId` is generated automatically as a UUID for each triggered run.

---

## Chaos Events

The event runs in **two phases**:

1. **Normal run** — no disruptions. Teams tune and test their pipeline.
2. **Chaos run** — start a run with **Enable Chaos Mode** checked in the Scoreboard control panel (or `"chaosEnabled":true` in the API trigger). The Scoreboard shows an amber **"Chaos Mode"** banner when chaos is armed, and a red pulsing banner when a specific event is in progress.

The organiser can fire specific chaos events during the run via the Judge chaos API (or via scripts). Teams know **what types of events can happen** (see below) but **not exactly when** they will be triggered — that is the challenge.

### Event Types

| Type | What happens | What your Processor must handle |
|---|---|---|
| `processor-restart` | Your Processor service is killed and restarts mid-run | Reconnect to MQTT, recover in-progress window state, resume publishing results |
| `message-burst` | Publisher briefly sends at a much faster rate | Dedup by `sequence`, avoid double-counting, handle backpressure |
| `message-gap` | Publisher pauses sending for several seconds | Keep open windows alive; do not close/discard windows prematurely |
| `device-dropout` | One device stops sending for a period | Finalise that device's windows with the data you have; don't block other devices |
| `high-latency` | Artificial delay injected between publisher and broker | Tighten your latency margin; publish results before the 2-second grace period expires |

### Resilience Tips

- **MQTT reconnect:** Register a reconnect handler and re-subscribe to the raw telemetry topic on reconnect. All in-progress window state should survive the reconnect.
- **State preservation across restarts:** Persist window aggregates to a fast local store (or re-derive from any buffered messages) so a restart does not wipe your windows.
- **Deduplication:** Always deduplicate by `runId|teamId|deviceId|sequence` — the Judge does this too; duplicates you publish as separate windows will be scored `Invalid`.
- **Window timing:** Use `eventTimeUtc` (not wall clock) for window assignment. A burst or gap in wall time should not shift your window boundaries.

---

## Agentic Assistance with Aspire MCP

The Aspire dashboard exposes an MCP server that lets AI assistants (such as GitHub Copilot) inspect resources, logs, traces, and health without guessing.

```bash
aspire agent init
```

Follow the prompts to configure your AI assistant. Useful prompts once connected:

- *Are all Aspire resources running?*
- *Show console logs for the processor.*
- *Show structured logs for the judge.*
- *List traces involving the scoreboard.*
