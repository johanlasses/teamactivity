# MQTT AI Battle - Implementation Specification

## 1. Purpose

This document defines the target design for improving `TeamActivity.Publisher` and `TeamActivity.Processor` for the MQTT AI Battle.

The implementation must maximize score while preserving all external contracts and keeping the solution runnable.

---

## 2. Goals

### Primary goals
1. Correct aggregate results
   - Correct `count`, `sum`, `min`, `max`, `avg`
   - Correct 5-second epoch-aligned window assignment
   - Correct handling of duplicate telemetry

2. High publish attainment
   - Publish all valid windows
   - Do not lose windows on normal completion
   - Do not lose windows on transient MQTT disconnect
   - Improve recovery after process kill or restart

3. Low latency
   - Publish results as close to `windowEnd` as safely possible
   - Remove coarse polling delays where practical

4. Chaos resilience
   - Handle duplicate telemetry
   - Recover from transient MQTT disconnects
   - Recover partially from process restarts

5. Scalability
   - Avoid unbounded memory growth
   - Reduce expensive global scans
   - Keep hot paths lightweight

---

## 3. Hard Constraints

### Must not change
- `src/TeamActivity.Judge`
- `src/TeamActivity.Scoreboard`
- `src/TeamActivity.Shared.Contracts`
- `TeamActivity.AppHost`
- `scripts/`

### Must preserve exactly
- MQTT topic patterns
- JSON field names
- schema version
- required Publisher control messages
- raw telemetry QoS contract
- result topic formatting
- code regions explicitly marked with `// ── KEEP THIS ──────────────────────────────────────────────────────────`

### Protected code markers
- Any code block explicitly marked with `// ── KEEP THIS ──────────────────────────────────────────────────────────` is protected and must not be modified.
- If a proposed design in this specification would require changing a protected block, treat the marker as the higher-priority constraint and adjust the implementation around it.

### Existing contract reminders
- Raw telemetry topic: `telemetry/v1/{runId}/{teamId}/raw`
- Control topics: `control/v1/{runId}/{teamId}/{event}`
- Result topic: `results/v1/{runId}/{teamId}/device/{deviceId}/window/{windowStartUtcMs}`
- Windows are fixed 5-second windows aligned to Unix epoch
- Deduplication key: `runId|teamId|deviceId|sequence`
- Results after `windowEnd + 2s` are invalid
- Publisher must emit `publisher-start` and `publisher-complete`

---

## 4. Current Problems To Fix

Based on the current code and repo guidance:

1. Processor state is too global and unsafe across run changes
2. Processor does not subscribe to Publisher control messages
3. Processor does not deduplicate telemetry
4. Processor publishes windows using a coarse periodic scan
5. Processor risks dropping or duplicating windows during flush or reconnect work
6. Processor has no cleanup strategy
7. Processor has no reconnect handling
8. Processor has no restart recovery
9. Publisher emits sequentially and may drift under load
10. Publisher has no reconnect handling
11. Publisher has no restart recovery

---

## 5. Design Overview

### 5.1 Publisher target design

Publisher remains a single hosted service, but gains:

1. Active run state
   - Track the currently running run in a dedicated in-memory object
   - Persist a minimal active-run checkpoint to disk

2. Reconnect-aware MQTT client lifecycle
   - Reconnect with backoff
   - Resubscribe to run-trigger and run-abort topics after reconnect

3. Improved scheduler
   - Use a stable scheduling reference for the full run
   - Reduce interval drift and inner-loop skew

4. Recoverable run execution
   - If the process restarts mid-run, restore active run state if still relevant
   - Resume from the next emission slot if possible

### 5.2 Processor target design

Processor remains a single hosted service, but gains:

1. Per-run state model
   - No global reset on run change
   - Multiple runs can coexist briefly if needed

2. Control-topic subscription
   - Consume `publisher-start`
   - Consume `publisher-complete`

3. Strict deduplication
   - Drop duplicate telemetry before aggregate accumulation

4. Per-window publish scheduling
   - Publish windows based on due time, not coarse 500 ms scans

5. Result idempotency
   - Each logical result can be published at most once

6. Checkpoint-based restart recovery
   - Persist enough state to recover in-progress windows after restart

7. Cleanup
   - Remove expired windows, dedupe entries, and published markers

---

## 6. Data Model

### 6.1 Publisher in-memory state

```csharp
ActiveRunState
{
    string RunId
    string TeamId
    int DeviceCount
    int IntervalMs
    int RunWindowSeconds
    DateTimeOffset RunStartedAtUtc
    DateTimeOffset RunEndsAtUtc
    long NextSequence
    int NextEmissionIndex
    bool StartControlPublished
    bool CompleteControlPublished
}
```

### Notes
- `NextSequence` is the next sequence to assign
- `NextEmissionIndex` is the next scheduled emission slot
- State is updated during run execution
- State is checkpointed periodically and on significant transitions

### 6.2 Processor in-memory state

```csharp
ProcessorState
{
    Dictionary<string, RunState> RunsByRunId
}
```

```csharp
RunState
{
    string RunId
    string TeamId
    DateTimeOffset? PublisherStartedAtUtc
    DateTimeOffset? PublisherCompletedAtUtc
    bool CompletionReceived

    Dictionary<WindowKey, WindowState> Windows
    HashSet<string> DedupeKeys
    HashSet<WindowKey> PublishedWindows

    DateTimeOffset LastUpdatedUtc
}
```

```csharp
WindowState
{
    WindowKey Key
    DateTimeOffset WindowStartUtc
    DateTimeOffset WindowEndUtc
    AggregateWindow Aggregate
    bool PublishScheduled
    bool Published
    DateTimeOffset FirstSeenUtc
    DateTimeOffset LastSeenUtc
}
```

### Notes
- `WindowKey` already exists in shared contracts
- `AggregateWindow` already provides accumulation behavior
- Published windows are guarded separately for idempotency

---

## 7. Processor Functional Design

### 7.1 Subscriptions

Processor subscribes to:
1. telemetry wildcard for this team
2. control wildcard for this team

Telemetry is used for aggregates. Control messages are used for run lifecycle awareness and flush behavior.

### 7.2 Telemetry processing flow

For each received telemetry message:

1. Deserialize
2. Validate required fields
3. Compute dedupe key: `runId|teamId|deviceId|sequence`
4. If duplicate, drop it
5. Resolve run state by `runId`
6. Compute epoch-aligned window from `eventTimeUtc`
7. Resolve or create `WindowState`
8. Add value to aggregate
9. Update scheduling metadata
10. Ensure the window is scheduled for publication

### 7.3 Window assignment rule

Windowing rule is:

```text
windowStart = floor(eventTimeUtc_unix_ms / 5000) * 5000
windowEnd = windowStart + 5000 ms
```

This must be used consistently everywhere.

### 7.4 Publish timing rule

#### Publish target
Publish a window at:

```text
windowEnd + publishHoldback
```

#### Default holdback
- `publishHoldback = 50 ms`

#### Reason
Publishing exactly at `windowEnd` is risky if the last telemetry for that window arrives slightly after due to transport jitter. A small holdback improves correctness while keeping latency competitive.

#### Constraint
A window must not be published later than:

```text
windowEnd + 2 seconds
```

### 7.5 Publish scheduling model

Use a single scheduler loop with a priority queue or min-heap of due windows.

#### Why
- Avoid scanning all windows every 500 ms
- Reduce latency
- Avoid per-window timer explosion

#### Model
- When a window is first seen, compute due time
- Add to the scheduler queue if not already scheduled
- Scheduler waits until the earliest due window
- Scheduler attempts publish when due
- If already published, skip
- On `publisher-complete`, bypass the scheduler and flush remaining windows immediately

### 7.6 Result idempotency

Before publishing a result, check:

```text
(runId, teamId, deviceId, windowStartUtc)
```

If already published, skip.

This guard must protect against:
- scheduler race
- completion flush race
- reconnect or replay race
- restart recovery race

### 7.7 Completion flush

When Processor receives `publisher-complete` for a run:

1. Mark the run as completed
2. Publish all unpublished windows for that run immediately
3. Still respect idempotency
4. Keep dedupe and published state long enough to avoid duplicates
5. Mark the run eligible for cleanup after a retention period

Completion flush is the authoritative end-of-run drain mechanism.

### 7.8 Late and out-of-order telemetry

#### Out-of-order
Allowed. Always assign based on `eventTimeUtc`, not arrival order.

#### Late arrival before publication
If the window is not yet published, it should still be included.

#### Late arrival after publication
If the window was already published, do not republish. The message can be ignored for scoring purposes.

#### Too-late result
Do not publish a new result after `windowEnd + 2s`.

### 7.9 Cleanup and retention

#### Windows
Keep until:
- published
- run completed or aged out
- retention expires

Suggested retention:
- `windowEnd + 10 seconds`

#### Dedupe keys
Keep until:
- run completed and aged out

Suggested retention:
- `max(windowEnd) + 10 seconds` or run cleanup time

#### Published markers
Keep until:
- run cleanup time

#### Run cleanup
Remove a run when:
- completion received and all known windows are published and retention elapsed
- or run state is stale beyond a safety timeout

Suggested stale timeout:
- `run end + 30 seconds`

---

## 8. Processor Restart Recovery

### 8.1 What to persist
Persist minimal checkpoint data for active runs:
- run metadata
- open windows with aggregate values
- dedupe keys that still matter
- published-window keys that still matter
- completion state
- last update timestamps

### 8.2 Checkpoint triggers
Checkpoint:
- periodically, for example every 250 ms or every N messages
- on `publisher-complete`
- after result publication
- on graceful shutdown

### 8.3 Storage
Use a local JSON checkpoint file under the Processor project working directory.

### 8.4 Restore behavior
On startup:
1. Load the checkpoint if present
2. Drop expired or stale runs
3. Restore active runs
4. Resume scheduling for unpublished windows
5. Reconnect and resubscribe

### 8.5 Limits
This is best-effort recovery. Messages lost while the process was down cannot be recreated.

---

## 9. Publisher Functional Design

### 9.1 Trigger handling
Publisher continues to consume:
- run trigger
- run abort

Run trigger creates a new `ActiveRunState`. Run abort cancels the active run.

### 9.2 Scheduling model

Publisher uses a stable schedule based on:

```text
scheduledAtUtc = runStartedAtUtc + emissionIndex * interval
```

For each emission slot:
1. Wait until due time
2. Emit one telemetry reading per device
3. Advance `NextEmissionIndex`
4. Periodically checkpoint progress

Improvement goal: minimize drift relative to the scheduled slot times.

### 9.3 Emission strategy

Preferred approach:
- Keep the outer emission-slot loop
- Optimize the inner device loop
- Precompute reusable data where safe
- Reduce allocation and serialization overhead
- Use bounded parallelism only if it is clearly safe

Non-goal: do not introduce overly complex concurrency that risks broken sequence behavior.

### 9.4 Sequence handling

Publisher sequence values must:
- remain unique per logical telemetry message
- survive reconnects
- survive restart recovery if an active run is resumed

Use `NextSequence` from the active run state.

### 9.5 Reconnect behavior

If MQTT disconnects:
1. Enter a reconnect loop with backoff
2. Reconnect
3. Resubscribe to trigger and abort topics
4. If a run is active, continue it from current state

Do not reset:
- active run
- emission index
- sequence

### 9.6 Restart recovery

Persist minimal active-run checkpoint:
- run identity
- run timing reference
- next emission index
- next sequence
- whether start or complete control was sent

On startup:
1. Load the checkpoint if present
2. If the run window is still active, resume safely
3. If already expired, do not resume telemetry
4. If necessary, send completion only if that is safe and clearly correct

---

## 10. Observability

Add lightweight logs and metrics for:

### Publisher
- connected
- disconnected
- reconnect attempt and success
- trigger received
- run started
- run resumed from checkpoint
- abort received
- completion sent
- checkpoint saved and loaded

### Processor
- connected
- disconnected
- reconnect attempt and success
- telemetry received count
- duplicate dropped count
- control received
- window created
- result published
- completion flush count
- checkpoint saved and loaded
- skipped duplicate result count
- stale cleanup count

Hot-path logs must be cheap and mostly aggregate or counter-based.

---

## 11. Validation Plan

### 11.1 Build validation
- Solution builds successfully

### 11.2 Smoke validation
- Short run starts
- Publisher emits telemetry
- Processor publishes results
- Scoreboard returns to idle

### 11.3 Correctness validation
- Aggregates are correct for a known small run
- Duplicate telemetry does not alter the aggregate
- Window topics match the expected window start
- `publisher-complete` flush works

### 11.4 Resilience validation
- transient broker disconnect reconnects cleanly
- Processor resumes scheduling after reconnect
- Publisher resumes active run after reconnect
- checkpoint restore works after process restart

### 11.5 Cleanup validation
- completed runs age out
- dedupe and published state are removed
- memory does not grow unbounded in longer tests

---

## 12. Non-Goals

The implementation will not:
- change external contracts
- modify Judge or Scoreboard behavior
- guarantee perfect recovery of messages lost while a process was fully down
- introduce distributed storage or complex external dependencies

---

## 13. Implementation Order

Even if merged as one branch or PR, implementation should follow this internal order:

1. Processor per-run state
2. Processor control subscription
3. Processor dedupe
4. Processor completion flush
5. Processor precise scheduler
6. Processor result idempotency
7. Processor late and out-of-order handling
8. Processor cleanup
9. Processor reconnect
10. Processor restart recovery
11. Publisher timing improvements
12. Publisher hot-path optimizations
13. Publisher reconnect
14. Publisher restart recovery
15. Observability
16. Final validation and tuning

---

## 14. Definition of Done

Done means all of the following are true:

1. Solution builds cleanly
2. Normal run works end to end
3. Processor deduplicates by `runId|teamId|deviceId|sequence`
4. Processor flushes on `publisher-complete`
5. Processor publishes near `windowEnd` with low added delay
6. Processor does not lose state on new run arrival
7. Processor reconnects and resubscribes
8. Processor restores useful state after restart
9. Publisher reconnects and resubscribes
10. Publisher restores active run state after restart
11. Memory cleanup exists for completed state
12. External contracts remain unchanged
13. Services remain runnable and debuggable