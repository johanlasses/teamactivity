---
applyTo: "src/TeamActivity.Processor/**,src/TeamActivity.Publisher/**"
---

# Chaos mode handling

When chaos mode is enabled, the following events can fire during a run. Your code must handle all three gracefully.

## Events

| Event | What happens | Required handling |
|---|---|---|
| `processor-disconnect` | Processor is killed mid-run | Reconnect to MQTT, recover in-progress window state from memory |
| `publisher-disconnect` | Publisher is killed mid-run | Reconnect to MQTT, resume publishing from where it left off |
| `message-duplications` | Duplicate messages injected into the telemetry stream | Deduplicate by `sequence` field using `WindowMath.TelemetryDedupeKey()` |

## Implementation guidance

### Reconnect recovery (processor-disconnect / publisher-disconnect)

- Use `MqttClient.DisconnectedAsync` event with exponential backoff.
- After reconnect, re-subscribe to topics immediately.
- Window state stored in `ConcurrentDictionary` survives the reconnect (in-memory state is not lost unless the process is fully terminated and restarted by Aspire).
- If the process is fully restarted: accept that in-progress windows may be lost — focus on correctly handling all new messages from the reconnect point forward.

### Deduplication (message-duplications)

- Track seen message keys using `WindowMath.TelemetryDedupeKey()` which produces `{runId}|{teamId}|{deviceId}|{sequence}`.
- Use a `HashSet<string>` or `HashSet<(string deviceId, long sequence)>` per run.
- Clear the dedup set when a new `runId` is detected (same pattern as existing window state reset).
- Duplicates should be silently discarded — do not count them in the aggregate window.
