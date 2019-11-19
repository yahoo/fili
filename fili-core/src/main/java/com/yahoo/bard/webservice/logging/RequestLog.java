// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import static com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl.DRUID_QUERY_ALL_TIMER;
import static com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl.DRUID_QUERY_MAX_TIMER;
import static com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl.DRUID_QUERY_TIMER;
import static com.yahoo.bard.webservice.util.StreamUtils.not;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.logging.blocks.Durations;
import com.yahoo.bard.webservice.logging.blocks.Preface;
import com.yahoo.bard.webservice.logging.blocks.Threads;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Represents the logging framework that provides timing capabilities of arbitrary phases on the handling lifecycle of a
 * request and accumulation of information for such a request in a single mega log line.
 */
public class RequestLog {

    private static final Logger LOG = LoggerFactory.getLogger(RequestLog.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final String ID_KEY = "logid";
    private static final long MS_PER_NS = 1000000;
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    private static final ThreadLocal<RequestLog> RLOG = ThreadLocal.withInitial(RequestLog::new);
    private static final String LOGINFO_ORDER_STRING = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("requestlog_loginfo_order"), ""
    );
    private static final List<String> LOGINFO_ORDER = generateLogInfoOrder(LOGINFO_ORDER_STRING);

    private String logId;
    private LogBlock info;
    @Deprecated
    private TimedPhase mostRecentTimer;
    private final Map<String, TimedPhase> times;
    private final Set<String> threadIds;

    /**
     * This class has only static methods and is not supposed to be directly instantiated.
     */
    private RequestLog() {
        logId = null;
        info = null;
        mostRecentTimer = null;
        times = new LinkedHashMap<>();
        threadIds = new LinkedHashSet<>();
        MDC.remove(ID_KEY);
    }

    /**
     * Copy constructor is also private.
     *
     * @param  rl request log object to copy from
     */
    private RequestLog(RequestLog rl) {
        logId = rl.logId;
        info = rl.info;
        mostRecentTimer = rl.mostRecentTimer;
        times = new LinkedHashMap<>(rl.times);
        threadIds = new LinkedHashSet<>(rl.threadIds);
        MDC.put(ID_KEY, logId);
    }

    /**
     * Resets the contents of a request log at the calling thread.
     */
    private void clear() {
        logId = null;
        info = null;
        mostRecentTimer = null;
        times.clear();
        threadIds.clear();
        MDC.remove(ID_KEY);
    }

    /**
     * Creates a new and empty request log at the calling thread.
     */
    private void init() {
        logId = UUID.randomUUID().toString();
        info = new LogBlock(logId);
        // Trick to place Durations and Threads in front of the Json while keep using a LinkedHashMap.
        // The actual entries will be replaced later when export is called but the initial order will be respected
        info.add(Durations.class);
        info.add(Threads.class);
        info.add(Preface.class);
        getLoginfoOrder().stream().filter(entry -> !Objects.equals(entry, "Epilogue")).forEachOrdered(
                entry -> {
                    try {
                        Class<? extends LogInfo> cls = Class.forName(entry).asSubclass(LogInfo.class);
                        info.add(cls);
                    } catch (ClassNotFoundException | ClassCastException e) {
                        String msg = ErrorMessageFormat.LOGINFO_CLASS_INVALID.logFormat(entry);
                        LOG.warn(msg, e);
                    }
                }
        );
        times.clear();
        threadIds.clear();
        threadIds.add(Thread.currentThread().getName());
        MDC.put(ID_KEY, logId);
    }

    /**
     * Get the aggregate durations for this request.
     *
     * @return A map of phase to duration (ns)
     */
    public static Map<String, Long> getDurations() {
        RequestLog log = RLOG.get();
        return log.durations();
    }

    /**
     * Adds the durations in milliseconds of all the recorded timed phases to a map.
     *
     * @return the map containing all the recorded times per phase in milliseconds
     */
    private Map<String, Long> durations() {
        return times.values().stream().collect(Collectors.toMap(TimedPhase::getName, TimedPhase::getDuration));
    }

    /**
     * Get the aggregate durations for this request.
     *
     * @return A map of phase to duration (ms)
     */
    public static Map<String, Float> getAggregateDurations() {
        return RLOG.get().aggregateDurations();
    }

    /**
     * Adds the durations in milliseconds of all the recorded timed phases to a map.
     *
     * @return the map containing all the recorded times per phase in milliseconds
     */
    private Map<String, Float> aggregateDurations() {
        Map<String, Long> durations = durations();

        OptionalLong max = durations.entrySet()
                .stream()
                .filter(e -> e.getKey().contains(DRUID_QUERY_TIMER))
                .mapToLong(Map.Entry::getValue)
                .peek(v -> REGISTRY.timer(DRUID_QUERY_ALL_TIMER).update(v, TimeUnit.NANOSECONDS))
                .max();

        if (max.isPresent()) {
            REGISTRY.timer(DRUID_QUERY_MAX_TIMER).update(max.getAsLong(), TimeUnit.NANOSECONDS);
            durations.put(DRUID_QUERY_MAX_TIMER, max.getAsLong());
        }

        return durations.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> (float) e.getValue() / MS_PER_NS));
    }

    /**
     * Check if a stopwatch is currently running.
     *
     * @param caller  the caller to name this stopwatch with its class's simple name
     *
     * @return whether this stopwatch is currently running
     */
    public static boolean isRunning(Object caller) {
        return isRunning(caller.getClass().getSimpleName());
    }

    /**
     * Check if a stopwatch is currently running.
     *
     * @param timePhaseName  the name of this stopwatch
     *
     * @return whether this stopwatch is started
     */
    public static boolean isRunning(String timePhaseName) {
        RequestLog current = RLOG.get();
        TimedPhase timePhase = current.times.get(timePhaseName);
        return timePhase != null && timePhase.isRunning();
    }

    /**
     * Start a stopwatch.
     * Time is accumulated if the stopwatch is already registered
     *
     * @param caller  the caller to name this stopwatch with its class's simple name
     *
     * @return The stopwatch
     */
    public static TimedPhase startTiming(Object caller) {
        return startTiming(caller.getClass().getSimpleName());
    }

    /**
     * Start a stopwatch.
     * Time is accumulated if the stopwatch is already registered
     *
     * @param timePhaseName  the name of this stopwatch
     *
     * @return The stopwatch
     */
    public static TimedPhase startTiming(String timePhaseName) {
        RequestLog current = RLOG.get();
        TimedPhase timePhase = current.times.get(timePhaseName);
        if (timePhase == null) {
            // If it was the first phase in general, create logging context as well
            if (current.info == null) {
                current.init();
            }

            timePhase = new TimedPhase(timePhaseName);
            current.times.put(timePhaseName, timePhase);
        }
        current.mostRecentTimer = timePhase;
        return timePhase.start();
    }

    /**
     * Stop the most recent stopwatch and start this one.
     * Time is accumulated if the stopwatch is already registered.
     *
     * @param nextPhase  the name of the stopwatch to be started
     *
     * @deprecated This method is too dependent on context that can be too easily changed by internal method calls.
     * Each timer should be explicitly started by {@link RequestLog#startTiming(String)} and stopped by
     * {@link RequestLog#stopTiming(String)} instead
     */
    @Deprecated
    public static void switchTiming(String nextPhase) {
        stopMostRecentTimer();
        startTiming(nextPhase);
    }

    /**
     * Pause a stopwatch.
     *
     * @param caller  the caller to name this stopwatch with its class's simple name
     */
    public static void stopTiming(Object caller) {
        stopTiming(caller.getClass().getSimpleName());
    }

    /**
     * Registers the final duration of a stopped timer with the RequestLog.
     *
     * @param stoppedPhase  The phase that has been stopped, and whose duration needs to be stored
     */
    public static void registerTime(TimedPhase stoppedPhase) {
        RequestLog.REGISTRY.timer(stoppedPhase.getName()).update(stoppedPhase.getDuration(), stoppedPhase.getUnit());
    }

    /**
     * Pause a stopwatch.
     *
     * @param timePhaseName  the name of this stopwatch
     */
    public static void stopTiming(String timePhaseName) {
        TimedPhase timePhase = RLOG.get().times.get(timePhaseName);
        if (timePhase == null) {
            LOG.warn("Tried to stop non-existent phase: {}", timePhaseName);
            return;
        }
        timePhase.close();
    }

    /**
     * Stop the most recent stopwatch.
     *
     * @deprecated  Stopping a timer based on context is brittle, and prone to unexpected changes. Timers should be
     * stopped explicitly, or started in a try-with-resources block
     */
    @Deprecated
    public static void stopMostRecentTimer() {
        try {
            stopTiming(RLOG.get().mostRecentTimer.getName());
        } catch (NullPointerException ignored) {
            LOG.warn("Stopping timing failed because mostRecentTimer wasn't registered.");
        }
    }

    /**
     * Record logging information in the logging context.
     *
     * @param logPhase  The name of the class destined to hold this logging information
     *
     * @see LogBlock
     */
    public static void record(LogInfo logPhase) {
        try {
            RLOG.get().info.add(logPhase);
        } catch (NullPointerException ignored) {
            LOG.warn(
                    "Attempted to append log info while request log object was uninitialized: {}",
                    logPhase.getClass().getSimpleName()
            );
        }
    }

    /**
     * Retrieve logging information in the logging context.
     *
     * @param cls  The class destined to hold this logging information
     *
     * @return the logging information in the logging context
     *
     * @see LogBlock
     */
    public static LogInfo retrieve(Class cls) {
        RequestLog requestLog = RLOG.get();
        if (requestLog == null) {
            String message = String.format(
                    "Attempted to retrieve log info while request log object was uninitialized: %s",
                    cls.getSimpleName()
            );
            LOG.error(message);
            throw new IllegalStateException(message);
        }

        LogInfo logInfo = requestLog.info.get(cls.getSimpleName());
        if (logInfo == null) {
            String message = ErrorMessageFormat.RESOURCE_RETRIEVAL_FAILURE.format(cls.getSimpleName());
            LOG.error(message);
            throw new IllegalStateException(message);
        }
        return logInfo;
    }

    /**
     * Returns a map of all the LogInfo blocks currently registered with this thread's RequestLog.
     *
     * @return A map of all the LogInfo objects registered to this thread's RequestLog
     */
    public static Map<String, LogInfo> retrieveAll() {
        RequestLog requestLog = RLOG.get();
        if (requestLog == null) {
            String message = String.format("Attempted to retrieve log info while request log object was uninitialized");
            LOG.error(message);
            throw new IllegalStateException(message);
        }
        return requestLog.info.any();
    }

    /**
     * Write the request log object of the current thread as JSON.
     * The thread's request log is cleared after a call to this method.
     */
    public static void log() {
        RequestLog current = RLOG.get();
        if (current.info == null) {
            LOG.warn("Attempted to log while request log object was uninitialized");
            return;
        }
        LOG.info(export());
        current.clear();
    }

    /**
     * Exports a snapshot of the request log of the current thread and also resets the request log for that thread.
     *
     * @return the log context of the current thread
     */
    public static RequestLog dump() {
        RequestLog current = RLOG.get();
        RequestLog copy = new RequestLog(current);
        current.clear();
        RLOG.remove();
        return copy;
    }

    /**
     * Exports a snapshot of the request log of the current thread without resetting the request log for that thread.
     *
     * @return the log context of the current thread
     */
    public static RequestLog copy() {
        RequestLog current = RLOG.get();
        return new RequestLog(current);
    }

    /**
     * Returns the id of this request log as a string.
     * If called on an empty request log context, it initializes it.
     *
     * @return the log id
     */
    public static String getId() {
        RequestLog current = RLOG.get();
        if (current.info == null) {
            current.init();
        }
        return current.logId;
    }

    /**
     * Prepend an id prefix to generated druid query id.
     *
     * @param idPrefix  Prefix for queryId sent to druid
     */
    public static void addIdPrefix(String idPrefix) {
        RequestLog current = RLOG.get();
        String newId = idPrefix + getId();
        current.info = current.info.withUuid(newId);
        current.logId = newId;
        MDC.put(ID_KEY, newId);
    }

    /**
     * Overwrite current thread's request log context.
     * It first clears the request log of the current thread and then fills it with the contents found in ctx
     *
     * @param ctx  the log context to restore to current thread's request log
     */
    public static void restore(RequestLog ctx) {
        RequestLog current = RLOG.get();
        current.clear();
        current.logId = ctx.logId;
        current.info = ctx.info;
        current.mostRecentTimer = ctx.mostRecentTimer;
        current.times.putAll(ctx.times);
        current.threadIds.addAll(ctx.threadIds);
        current.threadIds.add(Thread.currentThread().getName());
        MDC.put(ID_KEY, current.logId);
    }

    /**
     * Accumulates timings and threads to current thread's request log context.
     * It fills in the contents found in {@code ctx}. If the two contexts refer to different requests, then it logs a
     * warning and returns.
     *
     * @param ctx  The log context to accumulate to current thread's request log
     */
    public static void accumulate(RequestLog ctx) {
        RequestLog current = RLOG.get();
        if (!Objects.equals(current.logId, ctx.logId)) {
            LOG.warn(
                    "Tried to accumulate information to the current request: {} from a different request context: {}",
                    current.logId,
                    ctx.logId
            );
            return;
        }
        // Accumulate all the timers that are not currently running
        current.times.putAll(
                ctx.times.entrySet()
                        .stream()
                        .filter(
                                e -> e.getKey().contains(DRUID_QUERY_TIMER) ||
                                        (e.getKey().equals(REQUEST_WORKFLOW_TIMER) && !e.getValue().isRunning()) ||
                                        (e.getKey().equals(RESPONSE_WORKFLOW_TIMER) && e.getValue().isRunning())
                        )
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
        current.threadIds.addAll(ctx.threadIds);
        current.threadIds.add(Thread.currentThread().getName());
    }

    /**
     * Exports current thread's request log object as a formatted string without resetting it.
     *
     * @return log object as a formatted string
     */
    public static String export() {
        RequestLog current = RLOG.get();
        record(new Durations(current.aggregateDurations()));
        record(new Threads(current.threadIds));
        return LogFormatterProvider.getInstance().format(current.info);
    }

    private List<String> getLoginfoOrder() {
        return LOGINFO_ORDER;
    }

    /**
     * Get the logging blocks that need to be at the "head" of the log output.
     *
     * @param order  Indication of order of the initial blocks.
     *
     * @return the blocks in order
     */
    private static List<String> generateLogInfoOrder(String order) {
        return Arrays.stream(order.replaceAll("\\s+", "").split(","))
                .filter(not(String::isEmpty))
                .map(name -> "com.yahoo.bard.webservice.logging.blocks." + name)
                .collect(Collectors.toList());
    }
}
