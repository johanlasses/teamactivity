using System.Diagnostics.Metrics;
using System.IO;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using TeamActivity.Shared.Contracts;

namespace TeamActivity.Processor;

public sealed class Worker(
    ILogger<Worker> logger,
    IOptions<MqttOptions> mqttOptions,
    IOptions<ChallengeOptions> challengeOptions) : BackgroundService
{
    private const int PublishHoldbackMs = 50;
    private static readonly TimeSpan CheckpointInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan CleanupInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RetryPublishDelay = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan WindowRetention = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan RunStaleTimeout = TimeSpan.FromSeconds(30);
    private const string CheckpointFileName = "processor-checkpoint.json";

    private static readonly Counter<long> TelemetryConsumed = TelemetryMeters.Processor.CreateCounter<long>("telemetry_consumed_total");
    private static readonly Counter<long> ResultsPublished = TelemetryMeters.Processor.CreateCounter<long>("results_published_total");
    private static readonly Counter<long> DuplicateTelemetryDropped = TelemetryMeters.Processor.CreateCounter<long>("duplicate_telemetry_dropped_total");
    private static readonly Counter<long> ControlMessagesReceived = TelemetryMeters.Processor.CreateCounter<long>("control_messages_received_total");
    private static readonly Counter<long> CompletionFlushes = TelemetryMeters.Processor.CreateCounter<long>("completion_flush_total");
    private static readonly Counter<long> DuplicateResultsSkipped = TelemetryMeters.Processor.CreateCounter<long>("duplicate_results_skipped_total");
    private static readonly Counter<long> CheckpointsSaved = TelemetryMeters.Processor.CreateCounter<long>("processor_checkpoint_saved_total");
    private static readonly Counter<long> CheckpointsLoaded = TelemetryMeters.Processor.CreateCounter<long>("processor_checkpoint_loaded_total");
    private static readonly Counter<long> CleanupEvents = TelemetryMeters.Processor.CreateCounter<long>("processor_cleanup_total");

    private readonly object gate = new();
    private readonly SemaphoreSlim schedulerSignal = new(0, int.MaxValue);
    private readonly Dictionary<string, RunState> runsByRunId = [];
    private readonly PriorityQueue<ScheduledWindow, DateTimeOffset> publishQueue = new();

    private readonly Dictionary<WindowKey, AggregateWindow> windows = [];
    private readonly HashSet<WindowKey> publishedWindows = [];
    private string? currentRunId;

    private bool subscriptionsReady;
    private bool checkpointDirty;
    private DateTimeOffset lastCheckpointAtUtc = DateTimeOffset.MinValue;
    private DateTimeOffset lastCleanupAtUtc = DateTimeOffset.MinValue;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mqtt = mqttOptions.Value;
        var challenge = challengeOptions.Value;
        var telemetryWildcardTopic = Topics.TelemetryRawWildcard(challenge.TeamId);
        var controlWildcardTopic = $"control/v1/+/{challenge.TeamId}/+";

        RestoreCheckpoint(challenge);

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();
        var clientOptions = CreateClientOptions(mqtt, challenge.TeamId);

        client.ApplicationMessageReceivedAsync += args =>
        {
            var payload = Encoding.UTF8.GetString(args.ApplicationMessage.Payload);

            if (Topics.TryParseTelemetryRaw(args.ApplicationMessage.Topic, out _, out var telemetryTeamId)
                && telemetryTeamId == challenge.TeamId)
            {
                HandleTelemetry(args.ApplicationMessage.Topic, payload, challenge);
            }
            else if (Topics.TryParseControl(args.ApplicationMessage.Topic, out _, out var controlTeamId, out _)
                && controlTeamId == challenge.TeamId)
            {
                HandleControl(args.ApplicationMessage.Topic, payload, challenge);
            }

            return Task.CompletedTask;
        };

        client.DisconnectedAsync += args =>
        {
            subscriptionsReady = false;
            if (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(args.Exception, "Processor disconnected from MQTT broker.");
            }

            return Task.CompletedTask;
        };

        await EnsureConnectedAndSubscribedAsync(client, clientOptions, telemetryWildcardTopic, controlWildcardTopic, stoppingToken);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await EnsureConnectedAndSubscribedAsync(client, clientOptions, telemetryWildcardTopic, controlWildcardTopic, stoppingToken);

                List<PublishPlan> publishPlans;
                ProcessorCheckpoint? checkpointSnapshot;
                long cleanupCount;
                TimeSpan nextWait;

                lock (gate)
                {
                    var now = DateTimeOffset.UtcNow;
                    publishPlans = CollectPublishPlans(now, challenge.GraceSeconds);
                    cleanupCount = CleanupExpiredStateUnsafe(now);
                    checkpointSnapshot = ShouldCheckpoint(now) ? CreateCheckpointSnapshotUnsafe() : null;
                    nextWait = GetNextWaitUnsafe(now);
                }

                foreach (var publishPlan in publishPlans)
                {
                    await PublishPlanAsync(
                        client,
                        clientOptions,
                        telemetryWildcardTopic,
                        controlWildcardTopic,
                        publishPlan,
                        stoppingToken);
                }

                if (cleanupCount > 0)
                {
                    CleanupEvents.Add(cleanupCount);
                }

                if (checkpointSnapshot is not null)
                {
                    SaveCheckpointSnapshot(checkpointSnapshot);
                }

                var signaled = await schedulerSignal.WaitAsync(nextWait, stoppingToken);
                if (signaled)
                {
                    while (schedulerSignal.CurrentCount > 0)
                    {
                        await schedulerSignal.WaitAsync(TimeSpan.Zero, stoppingToken);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            SaveCheckpointSnapshot(CreateCheckpointSnapshot());
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        SaveCheckpointSnapshot(CreateCheckpointSnapshot());
        await base.StopAsync(cancellationToken);
    }

    private void HandleTelemetry(string topic, string payload, ChallengeOptions challenge)
    {
        TelemetryMessage? telemetry;
        try
        {
            telemetry = JsonSerializer.Deserialize<TelemetryMessage>(payload, JsonContract.Options);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Processor ignored invalid telemetry JSON on {Topic}", topic);
            return;
        }

        if (telemetry is null || telemetry.SchemaVersion != Topics.SchemaVersion)
        {
            logger.LogWarning("Processor ignored unsupported telemetry payload on {Topic}", topic);
            return;
        }

        if (telemetry.TeamId != challenge.TeamId)
        {
            logger.LogWarning("Processor ignored telemetry for team {TeamId}", telemetry.TeamId);
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var windowKey = WindowMath.Assign(telemetry, challenge.WindowSeconds);
        var dedupeKey = WindowMath.TelemetryDedupeKey(telemetry);
        var accepted = false;
        var duplicateDropped = false;

        lock (gate)
        {
            // ── KEEP THIS ──────────────────────────────────────────────────────────
            // Resets window state when a new run starts so previous run data doesn't
            // bleed into the new run's aggregates. If you refactor state management,
            // make sure this reset still happens when the runId changes.
            if (currentRunId is not null && currentRunId != telemetry.RunId)
            {
                logger.LogInformation("New runId detected ({NewRunId}), clearing window state from previous run.", telemetry.RunId);
                windows.Clear();
                publishedWindows.Clear();
            }
            // ───────────────────────────────────────────────────────────────────────

            currentRunId = telemetry.RunId;

            var runState = GetOrCreateRunStateUnsafe(telemetry.RunId, telemetry.TeamId, now);
            runState.LastUpdatedUtc = now;
            runState.MaxWindowEndUtc = Max(runState.MaxWindowEndUtc, windowKey.WindowEndUtc);

            if (!runState.DedupeKeys.Add(dedupeKey))
            {
                duplicateDropped = true;
            }
            else if (runState.PublishedWindows.Contains(windowKey))
            {
                duplicateDropped = true;
            }
            else
            {
                if (!runState.Windows.TryGetValue(windowKey, out var windowState))
                {
                    windowState = new WindowState(windowKey, now, windowKey.WindowEndUtc.AddMilliseconds(PublishHoldbackMs));
                    runState.Windows.Add(windowKey, windowState);
                    ScheduleWindowUnsafe(windowState);
                    logger.LogDebug(
                        "Processor created window for run {RunId} device {DeviceId} at {WindowStartUtc}.",
                        telemetry.RunId,
                        telemetry.DeviceId,
                        windowKey.WindowStartUtc);
                }

                if (windowState.Published)
                {
                    duplicateDropped = true;
                }
                else
                {
                    windowState.Aggregate.Add(telemetry.Value);
                    windowState.LastSeenUtc = now;
                    accepted = true;
                    checkpointDirty = true;

                    if (!windows.TryGetValue(windowKey, out var shadowAggregate))
                    {
                        shadowAggregate = new AggregateWindow();
                        windows.Add(windowKey, shadowAggregate);
                    }

                    shadowAggregate.Add(telemetry.Value);
                }
            }
        }

        if (duplicateDropped)
        {
            DuplicateTelemetryDropped.Add(1, new KeyValuePair<string, object?>("team_id", telemetry.TeamId));
            return;
        }

        if (accepted)
        {
            TelemetryConsumed.Add(1, new KeyValuePair<string, object?>("team_id", telemetry.TeamId));
            SignalScheduler();
        }
    }

    private void HandleControl(string topic, string payload, ChallengeOptions challenge)
    {
        ControlMessage? control;
        try
        {
            control = JsonSerializer.Deserialize<ControlMessage>(payload, JsonContract.Options);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Processor ignored invalid control JSON on {Topic}", topic);
            return;
        }

        if (control is null || control.SchemaVersion != Topics.SchemaVersion)
        {
            logger.LogWarning("Processor ignored unsupported control payload on {Topic}", topic);
            return;
        }

        if (control.TeamId != challenge.TeamId)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var shouldWake = false;

        lock (gate)
        {
            var runState = GetOrCreateRunStateUnsafe(control.RunId, control.TeamId, now);
            runState.LastUpdatedUtc = now;

            if (control.Event == Topics.PublisherStart)
            {
                runState.PublisherStartedAtUtc = control.PublishedAtUtc;
                checkpointDirty = true;
                logger.LogInformation("Processor observed publisher-start for run {RunId}.", control.RunId);
            }
            else if (control.Event == Topics.PublisherComplete)
            {
                runState.PublisherCompletedAtUtc = control.PublishedAtUtc;
                runState.CompletionReceived = true;
                checkpointDirty = true;
                shouldWake = true;
                logger.LogInformation("Processor observed publisher-complete for run {RunId}.", control.RunId);
            }
            else
            {
                return;
            }
        }

        ControlMessagesReceived.Add(1, new KeyValuePair<string, object?>("event", control.Event));
        if (shouldWake)
        {
            CompletionFlushes.Add(1, new KeyValuePair<string, object?>("team_id", control.TeamId));
            SignalScheduler();
        }
    }

    private List<PublishPlan> CollectPublishPlans(DateTimeOffset now, int graceSeconds)
    {
        List<PublishPlan> publishPlans = [];
        HashSet<WindowKey> plannedKeys = [];

        while (publishQueue.TryPeek(out var scheduledWindow, out var dueAtUtc) && dueAtUtc <= now)
        {
            publishQueue.Dequeue();
            if (TryStartPublishUnsafe(scheduledWindow.RunId, scheduledWindow.Key, now, graceSeconds, bypassDueTime: false, out var publishPlan)
                && plannedKeys.Add(publishPlan.Key))
            {
                publishPlans.Add(publishPlan);
            }
        }

        foreach (var runState in runsByRunId.Values)
        {
            if (!runState.CompletionReceived)
            {
                continue;
            }

            foreach (var windowState in runState.Windows.Values)
            {
                if (TryStartPublishUnsafe(runState.RunId, windowState.Key, now, graceSeconds, bypassDueTime: true, out var publishPlan)
                    && plannedKeys.Add(publishPlan.Key))
                {
                    publishPlans.Add(publishPlan);
                }
            }
        }

        return publishPlans;
    }

    private bool TryStartPublishUnsafe(
        string runId,
        WindowKey key,
        DateTimeOffset now,
        int graceSeconds,
        bool bypassDueTime,
        out PublishPlan publishPlan)
    {
        publishPlan = default;

        if (!runsByRunId.TryGetValue(runId, out var runState)
            || !runState.Windows.TryGetValue(key, out var windowState))
        {
            return false;
        }

        if (windowState.Published || windowState.Publishing || windowState.Expired || runState.PublishedWindows.Contains(key))
        {
            return false;
        }

        if (!bypassDueTime && now < windowState.PublishDueAtUtc)
        {
            return false;
        }

        if (now > key.WindowEndUtc.AddSeconds(graceSeconds))
        {
            windowState.Expired = true;
            checkpointDirty = true;
            DuplicateResultsSkipped.Add(1, new KeyValuePair<string, object?>("team_id", key.TeamId));
            return false;
        }

        windowState.Publishing = true;
        windowState.PublishScheduled = false;
        publishPlan = new PublishPlan(runId, key);
        return true;
    }

    private async Task PublishPlanAsync(
        IMqttClient client,
        MqttClientOptions clientOptions,
        string telemetryWildcardTopic,
        string controlWildcardTopic,
        PublishPlan publishPlan,
        CancellationToken cancellationToken)
    {
        AggregateResultMessage result;
        string topic;

        lock (gate)
        {
            if (!TryCreateResultUnsafe(publishPlan, out result, out topic))
            {
                return;
            }
        }

        try
        {
            await EnsureConnectedAndSubscribedAsync(client, clientOptions, telemetryWildcardTopic, controlWildcardTopic, cancellationToken);

            var json = JsonSerializer.Serialize(result, JsonContract.Options);
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(json)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await client.PublishAsync(message, cancellationToken);

            lock (gate)
            {
                if (runsByRunId.TryGetValue(publishPlan.RunId, out var runState)
                    && runState.Windows.TryGetValue(publishPlan.Key, out var windowState))
                {
                    windowState.Publishing = false;
                    windowState.Published = true;
                    runState.PublishedWindows.Add(publishPlan.Key);
                    publishedWindows.Add(publishPlan.Key);
                    runState.LastUpdatedUtc = DateTimeOffset.UtcNow;
                    checkpointDirty = true;
                }
            }

            ResultsPublished.Add(1, new KeyValuePair<string, object?>("team_id", result.TeamId));
            logger.LogInformation(
                "Window reported: Device={DeviceId} [{WindowStart:HH:mm:ss}-{WindowEnd:HH:mm:ss}] Count={Count} Avg={Avg:F2} Min={Min:F2} Max={Max:F2}",
                result.DeviceId,
                result.WindowStartUtc,
                result.WindowEndUtc,
                result.Count,
                result.Avg,
                result.Min,
                result.Max);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            lock (gate)
            {
                ResetPublishingUnsafe(publishPlan, reschedule: true);
            }

            throw;
        }
        catch (Exception ex)
        {
            lock (gate)
            {
                ResetPublishingUnsafe(publishPlan, reschedule: true);
            }

            logger.LogWarning(ex, "Processor failed to publish result for run {RunId} device {DeviceId}; will retry.", publishPlan.RunId, publishPlan.Key.DeviceId);
            SignalScheduler();
        }
    }

    private bool TryCreateResultUnsafe(PublishPlan publishPlan, out AggregateResultMessage result, out string topic)
    {
        result = default!;
        topic = string.Empty;

        if (!runsByRunId.TryGetValue(publishPlan.RunId, out var runState)
            || !runState.Windows.TryGetValue(publishPlan.Key, out var windowState))
        {
            return false;
        }

        if (windowState.Published || runState.PublishedWindows.Contains(publishPlan.Key))
        {
            windowState.Publishing = false;
            DuplicateResultsSkipped.Add(1, new KeyValuePair<string, object?>("team_id", publishPlan.Key.TeamId));
            return false;
        }

        result = new AggregateResultMessage(
            Topics.SchemaVersion,
            publishPlan.Key.RunId,
            publishPlan.Key.TeamId,
            publishPlan.Key.DeviceId,
            publishPlan.Key.WindowStartUtc,
            publishPlan.Key.WindowEndUtc,
            windowState.Aggregate.Count,
            windowState.Aggregate.Sum,
            windowState.Aggregate.Min,
            windowState.Aggregate.Max,
            windowState.Aggregate.Avg,
            WindowMath.ResultId(publishPlan.Key),
            DateTimeOffset.UtcNow);

        topic = Topics.Result(
            publishPlan.Key.RunId,
            publishPlan.Key.TeamId,
            publishPlan.Key.DeviceId,
            publishPlan.Key.WindowStartUtc);
        return true;
    }

    private void ResetPublishingUnsafe(PublishPlan publishPlan, bool reschedule)
    {
        if (!runsByRunId.TryGetValue(publishPlan.RunId, out var runState)
            || !runState.Windows.TryGetValue(publishPlan.Key, out var windowState))
        {
            return;
        }

        if (windowState.Published)
        {
            return;
        }

        windowState.Publishing = false;
        if (reschedule)
        {
            ScheduleWindowUnsafe(windowState, DateTimeOffset.UtcNow + RetryPublishDelay);
        }
    }

    private long CleanupExpiredStateUnsafe(DateTimeOffset now)
    {
        if (lastCleanupAtUtc != DateTimeOffset.MinValue && now - lastCleanupAtUtc < CleanupInterval)
        {
            return 0;
        }

        long removed = 0;
        List<string> runsToRemove = [];

        foreach (var runState in runsByRunId.Values)
        {
            List<WindowKey> windowsToRemove = [];
            foreach (var (key, windowState) in runState.Windows)
            {
                if ((windowState.Published || windowState.Expired)
                    && now >= key.WindowEndUtc + WindowRetention)
                {
                    windowsToRemove.Add(key);
                }
            }

            foreach (var key in windowsToRemove)
            {
                runState.Windows.Remove(key);
                runState.PublishedWindows.Remove(key);
                removed++;
                checkpointDirty = true;
            }

            var completionBasis = runState.PublisherCompletedAtUtc ?? runState.MaxWindowEndUtc ?? runState.LastUpdatedUtc;
            if ((runState.CompletionReceived && runState.Windows.Count == 0 && now >= completionBasis + RunStaleTimeout)
                || (!runState.CompletionReceived && now >= runState.LastUpdatedUtc + RunStaleTimeout))
            {
                runsToRemove.Add(runState.RunId);
            }
        }

        foreach (var runId in runsToRemove)
        {
            runsByRunId.Remove(runId);
            removed++;
            checkpointDirty = true;
        }

        lastCleanupAtUtc = now;
        return removed;
    }

    private bool ShouldCheckpoint(DateTimeOffset now)
        => checkpointDirty && (lastCheckpointAtUtc == DateTimeOffset.MinValue || now - lastCheckpointAtUtc >= CheckpointInterval);

    private TimeSpan GetNextWaitUnsafe(DateTimeOffset now)
    {
        var wait = CheckpointInterval;
        if (publishQueue.TryPeek(out _, out var dueAtUtc))
        {
            var dueDelay = dueAtUtc - now;
            if (dueDelay < wait)
            {
                wait = dueDelay;
            }
        }

        if (wait <= TimeSpan.Zero)
        {
            return TimeSpan.FromMilliseconds(1);
        }

        return wait;
    }

    private void ScheduleWindowUnsafe(WindowState windowState, DateTimeOffset? dueAtUtc = null)
    {
        if (windowState.PublishScheduled || windowState.Published || windowState.Expired)
        {
            return;
        }

        windowState.PublishScheduled = true;
        publishQueue.Enqueue(new ScheduledWindow(windowState.Key.RunId, windowState.Key), dueAtUtc ?? windowState.PublishDueAtUtc);
    }

    private void SignalScheduler()
    {
        try
        {
            schedulerSignal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private RunState GetOrCreateRunStateUnsafe(string runId, string teamId, DateTimeOffset now)
    {
        if (!runsByRunId.TryGetValue(runId, out var runState))
        {
            runState = new RunState(runId, teamId, now);
            runsByRunId.Add(runId, runState);
        }

        return runState;
    }

    private async Task EnsureConnectedAndSubscribedAsync(
        IMqttClient client,
        MqttClientOptions clientOptions,
        string telemetryWildcardTopic,
        string controlWildcardTopic,
        CancellationToken cancellationToken)
    {
        if (client.IsConnected && subscriptionsReady)
        {
            return;
        }

        var attempt = 0;
        var delayMs = 100;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!client.IsConnected)
                {
                    logger.LogInformation(
                        attempt == 0
                            ? "Connecting processor to MQTT broker {Host}:{Port}"
                            : "Processor reconnect attempt {Attempt} to MQTT broker {Host}:{Port}",
                        mqttOptions.Value.Host,
                        mqttOptions.Value.Port,
                        attempt);
                    await client.ConnectAsync(clientOptions, cancellationToken);
                    logger.LogInformation("Processor connected to MQTT broker.");
                }

                var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(filter => filter
                        .WithTopic(telemetryWildcardTopic)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce))
                    .WithTopicFilter(filter => filter
                        .WithTopic(controlWildcardTopic)
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                    .Build();

                await client.SubscribeAsync(subscribeOptions, cancellationToken);
                subscriptionsReady = true;
                if (attempt > 0)
                {
                    logger.LogInformation("Processor resubscribed after reconnect.");
                }

                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                attempt++;
                subscriptionsReady = false;
                logger.LogWarning(ex, "Processor connect attempt {Attempt} failed; retrying.", attempt);
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), cancellationToken);
                delayMs = Math.Min(delayMs * 2, 5000);
            }
        }
    }

    private void RestoreCheckpoint(ChallengeOptions challenge)
    {
        var checkpointPath = GetCheckpointPath();
        if (!File.Exists(checkpointPath))
        {
            return;
        }

        try
        {
            var checkpoint = JsonSerializer.Deserialize<ProcessorCheckpoint>(File.ReadAllText(checkpointPath), JsonContract.Options);
            if (checkpoint is null)
            {
                return;
            }

            var now = DateTimeOffset.UtcNow;
            lock (gate)
            {
                foreach (var runCheckpoint in checkpoint.Runs)
                {
                    if (runCheckpoint.TeamId != challenge.TeamId)
                    {
                        continue;
                    }

                    var completionBasis = runCheckpoint.PublisherCompletedAtUtc ?? runCheckpoint.MaxWindowEndUtc ?? runCheckpoint.LastUpdatedUtc;
                    if (now >= completionBasis + RunStaleTimeout)
                    {
                        continue;
                    }

                    var runState = new RunState(runCheckpoint.RunId, runCheckpoint.TeamId, runCheckpoint.LastUpdatedUtc)
                    {
                        PublisherStartedAtUtc = runCheckpoint.PublisherStartedAtUtc,
                        PublisherCompletedAtUtc = runCheckpoint.PublisherCompletedAtUtc,
                        CompletionReceived = runCheckpoint.CompletionReceived,
                        LastUpdatedUtc = runCheckpoint.LastUpdatedUtc,
                        MaxWindowEndUtc = runCheckpoint.MaxWindowEndUtc
                    };

                    foreach (var dedupeKey in runCheckpoint.DedupeKeys)
                    {
                        runState.DedupeKeys.Add(dedupeKey);
                    }

                    foreach (var publishedKeyCheckpoint in runCheckpoint.PublishedWindows)
                    {
                        var publishedKey = publishedKeyCheckpoint.ToWindowKey(runCheckpoint.RunId, runCheckpoint.TeamId);
                        runState.PublishedWindows.Add(publishedKey);
                    }

                    foreach (var windowCheckpoint in runCheckpoint.Windows)
                    {
                        var windowState = windowCheckpoint.ToState(runCheckpoint.RunId, runCheckpoint.TeamId);
                        var windowKey = windowState.Key;

                        runState.Windows[windowKey] = windowState;
                        runState.MaxWindowEndUtc = Max(runState.MaxWindowEndUtc, windowKey.WindowEndUtc);

                        if (!windowState.Published && !windowState.Expired)
                        {
                            ScheduleWindowUnsafe(windowState, windowState.PublishDueAtUtc <= now ? now : windowState.PublishDueAtUtc);
                        }
                    }

                    if (runState.Windows.Count > 0 || runState.DedupeKeys.Count > 0 || runState.PublishedWindows.Count > 0)
                    {
                        runsByRunId[runState.RunId] = runState;
                    }
                }

                checkpointDirty = false;
                lastCheckpointAtUtc = now;
            }

            CheckpointsLoaded.Add(1);
            logger.LogInformation("Loaded processor checkpoint from {Path}.", checkpointPath);
        }
        catch (Exception ex) when (ex is IOException or JsonException)
        {
            logger.LogWarning(ex, "Failed to load processor checkpoint; starting clean.");
        }
    }

    private ProcessorCheckpoint CreateCheckpointSnapshot()
    {
        lock (gate)
        {
            return CreateCheckpointSnapshotUnsafe();
        }
    }

    private ProcessorCheckpoint CreateCheckpointSnapshotUnsafe()
    {
        return new ProcessorCheckpoint(
            [
                .. runsByRunId.Values.Select(runState => new RunCheckpoint(
                    runState.RunId,
                    runState.TeamId,
                    runState.PublisherStartedAtUtc,
                    runState.PublisherCompletedAtUtc,
                    runState.CompletionReceived,
                    runState.LastUpdatedUtc,
                    runState.MaxWindowEndUtc,
                    [.. runState.DedupeKeys],
                    [.. runState.PublishedWindows.Select(WindowKeyCheckpoint.FromWindowKey)],
                    [.. runState.Windows.Values.Select(WindowStateCheckpoint.FromState)]))
            ]);
    }

    private void SaveCheckpointSnapshot(ProcessorCheckpoint checkpoint)
    {
        try
        {
            var checkpointPath = GetCheckpointPath();
            var tempPath = $"{checkpointPath}.tmp";
            File.WriteAllText(tempPath, JsonSerializer.Serialize(checkpoint, JsonContract.Options));
            File.Move(tempPath, checkpointPath, overwrite: true);
            lastCheckpointAtUtc = DateTimeOffset.UtcNow;
            checkpointDirty = false;
            CheckpointsSaved.Add(1);
        }
        catch (Exception ex) when (ex is IOException or JsonException)
        {
            logger.LogWarning(ex, "Failed to save processor checkpoint.");
        }
    }

    private string GetCheckpointPath()
        => Path.Combine(Environment.CurrentDirectory, CheckpointFileName);

    private static MqttClientOptions CreateClientOptions(MqttOptions mqtt, string teamId)
    {
        return new MqttClientOptionsBuilder()
            .WithClientId($"processor-{teamId}")
            .WithTcpServer(mqtt.Host, mqtt.Port)
            .WithCleanStart()
            .Build();
    }

    private static DateTimeOffset? Max(DateTimeOffset? left, DateTimeOffset right)
    {
        if (left is null)
        {
            return right;
        }

        return left.Value >= right ? left.Value : right;
    }

    private sealed class RunState(string runId, string teamId, DateTimeOffset createdAtUtc)
    {
        public string RunId { get; } = runId;

        public string TeamId { get; } = teamId;

        public DateTimeOffset? PublisherStartedAtUtc { get; set; }

        public DateTimeOffset? PublisherCompletedAtUtc { get; set; }

        public bool CompletionReceived { get; set; }

        public Dictionary<WindowKey, WindowState> Windows { get; } = [];

        public HashSet<string> DedupeKeys { get; } = [];

        public HashSet<WindowKey> PublishedWindows { get; } = [];

        public DateTimeOffset LastUpdatedUtc { get; set; } = createdAtUtc;

        public DateTimeOffset? MaxWindowEndUtc { get; set; }
    }

    private sealed class WindowState(WindowKey key, DateTimeOffset firstSeenUtc, DateTimeOffset publishDueAtUtc)
    {
        public WindowKey Key { get; } = key;

        public AggregateWindow Aggregate { get; } = new();

        public bool PublishScheduled { get; set; }

        public bool Publishing { get; set; }

        public bool Published { get; set; }

        public bool Expired { get; set; }

        public DateTimeOffset FirstSeenUtc { get; } = firstSeenUtc;

        public DateTimeOffset LastSeenUtc { get; set; } = firstSeenUtc;

        public DateTimeOffset PublishDueAtUtc { get; } = publishDueAtUtc;
    }

    private readonly record struct ScheduledWindow(string RunId, WindowKey Key);

    private readonly record struct PublishPlan(string RunId, WindowKey Key);

    private sealed record ProcessorCheckpoint(List<RunCheckpoint> Runs);

    private sealed record RunCheckpoint(
        string RunId,
        string TeamId,
        DateTimeOffset? PublisherStartedAtUtc,
        DateTimeOffset? PublisherCompletedAtUtc,
        bool CompletionReceived,
        DateTimeOffset LastUpdatedUtc,
        DateTimeOffset? MaxWindowEndUtc,
        List<string> DedupeKeys,
        List<WindowKeyCheckpoint> PublishedWindows,
        List<WindowStateCheckpoint> Windows);

    private sealed record WindowKeyCheckpoint(
        string DeviceId,
        DateTimeOffset WindowStartUtc,
        DateTimeOffset WindowEndUtc)
    {
        public static WindowKeyCheckpoint FromWindowKey(WindowKey key)
            => new(key.DeviceId, key.WindowStartUtc, key.WindowEndUtc);

        public WindowKey ToWindowKey(string runId, string teamId)
            => new(runId, teamId, DeviceId, WindowStartUtc, WindowEndUtc);
    }

    private sealed record WindowStateCheckpoint(
        string DeviceId,
        DateTimeOffset WindowStartUtc,
        DateTimeOffset WindowEndUtc,
        int Count,
        double Sum,
        double Min,
        double Max,
        bool Published,
        bool Expired,
        DateTimeOffset FirstSeenUtc,
        DateTimeOffset LastSeenUtc,
        DateTimeOffset PublishDueAtUtc)
    {
        public static WindowStateCheckpoint FromState(WindowState state)
            => new(
                state.Key.DeviceId,
                state.Key.WindowStartUtc,
                state.Key.WindowEndUtc,
                state.Aggregate.Count,
                state.Aggregate.Sum,
                state.Aggregate.Min,
                state.Aggregate.Max,
                state.Published,
                state.Expired,
                state.FirstSeenUtc,
                state.LastSeenUtc,
                state.PublishDueAtUtc);

        public WindowKey ToWindowKey(string runId, string teamId)
            => new(runId, teamId, DeviceId, WindowStartUtc, WindowEndUtc);

        public WindowState ToState(string runId, string teamId)
        {
            var key = new WindowKey(runId, teamId, DeviceId, WindowStartUtc, WindowEndUtc);
            var state = new WindowState(key, FirstSeenUtc, PublishDueAtUtc)
            {
                Published = Published,
                Expired = Expired,
                LastSeenUtc = LastSeenUtc
            };

            foreach (var value in ExpandAggregateValues(Count, Sum, Min, Max))
            {
                state.Aggregate.Add(value);
            }

            return state;
        }

        private static IEnumerable<double> ExpandAggregateValues(int count, double sum, double min, double max)
        {
            if (count <= 0)
            {
                yield break;
            }

            if (count == 1)
            {
                yield return sum;
                yield break;
            }

            if (count == 2)
            {
                yield return min;
                yield return max;
                yield break;
            }

            yield return min;
            yield return max;

            var middleCount = count - 2;
            var middleValue = (sum - min - max) / middleCount;
            for (var index = 0; index < middleCount; index++)
            {
                yield return middleValue;
            }
        }
    }
}