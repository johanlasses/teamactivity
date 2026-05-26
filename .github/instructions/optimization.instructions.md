---
applyTo: "src/TeamActivity.Processor/**,src/TeamActivity.Publisher/**"
---

# Optimization best practices

Sources: mosquitto.org/man/mosquitto-conf-5.html, eclipse-mosquitto/mosquitto GitHub, dotnet/MQTTnet v5.x, learn.microsoft.com/dotnet/core/whats-new/dotnet-10.

## Mosquitto broker

- **QoS strategy**: QoS 0 for high-frequency telemetry (no PUBACK round-trip). QoS 1 for control messages and aggregate results. Never use QoS 2 (4-message handshake kills throughput).
- **TCP_NODELAY**: Enable `set_tcp_nodelay true` in `TeamActivity.AppHost/mosquitto/mosquitto.conf` — avoids Nagle coalescing, reduces per-message latency for small 50ms-interval payloads.
- **Wildcard subscriptions**: Use a single wildcard subscription on the Processor rather than per-device subscriptions. Already implemented via `Topics.TelemetryRawWildcard()`.
- **Reconnect strategy**: Use exponential backoff with random jitter to avoid thundering-herd reconnection of many devices simultaneously.
- **Inflight messages**: Default `max_inflight_messages` is 20. Design the processor to handle out-of-order QoS 0 messages using event timestamps, not delivery order.
- **Monitoring**: Subscribe to `$SYS/broker/load/publish/dropped/1min` to detect message drops from overload.

## MQTTnet client architecture

- Use raw `MqttClient` (not `ManagedMqttClient`) for maximum throughput (~150k msg/sec benchmarked). You own reconnect logic.
- **Register message handler BEFORE `ConnectAsync`** so no messages are lost during subscription setup.
- Use `DisconnectedAsync` event for reconnection with exponential backoff + re-subscribe.
- For QoS 1 results: set `ea.AutoAcknowledge = false` and call `ea.AcknowledgeAsync()` after processing to avoid premature ACK.

## Pipeline architecture

- **System.Threading.Channels**: Decouple the MQTT receive loop from processing. Use `Channel.CreateBounded<T>` with `SingleWriter=true`, `AllowSynchronousContinuations=false`, `FullMode=BoundedChannelFullMode.DropOldest`. The MQTT callback does `channel.Writer.TryWrite(msg)` and returns immediately — never block the receive loop.
- **ConcurrentDictionary<string, DeviceWindow>**: Use `GetOrAdd` (with `TArg` overload to avoid closure allocations) for lock-free per-device window state. ValueFactory runs outside the lock — idempotent creation is required.
- **PeriodicTimer**: Use for window flush loops. Guarantees no overlapping callbacks, supports `async/await`, accepts `CancellationToken`, and is injectable via `TimeProvider` constructor for unit tests.

## Memory efficiency

- **ArrayPool<byte>.Shared**: Rent buffers for JSON serialization of aggregate results. Return after publish. Eliminates per-flush GC pressure.
- **ObjectPool<T>** (Microsoft.Extensions.ObjectPool): Recycle DeviceWindow objects across flush cycles instead of allocating/GCing every 5 seconds.
- **Span<T> for synchronous paths**: Use `PayloadSegment.AsSpan()` for zero-copy deserialization within the message handler.
- **Memory<T> for async paths**: Use when buffer must survive across `await` boundaries.

## Observability

- ServiceDefaults already wires OpenTelemetry metrics/tracing with OTLP export.
- Add MQTT-specific instruments: `telemetry_consumed_total`, `results_published_total`, `windows_flushed`, `window_latency_ms` (histogram), `channel_backlog` (gauge), `messages_dropped` (counter for channel overflow).
- Add an MQTT health check (`IHealthCheck`) that reports `Unhealthy` when disconnected.
