// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.logging.RequestLog.TimedPhase;
import com.yahoo.bard.webservice.logging.blocks.Durations;
import com.yahoo.bard.webservice.logging.blocks.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl.DRUID_QUERY_TIMER;
import static com.yahoo.bard.webservice.logging.RequestLog.LOG_ID_KEY;
import static com.yahoo.bard.webservice.util.StreamUtils.not;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.REQUEST_WORKFLOW_TIMER;
import static com.yahoo.bard.webservice.web.handlers.workflow.DruidWorkflow.RESPONSE_WORKFLOW_TIMER;

/**
 * An interface for application code to interact with the {@link RequestLog}.
 */
public class RequestLogUtils {
    private static final String TOTAL_TIMER = "TotalTime";
    private static final Logger LOG = LoggerFactory.getLogger(RequestLog.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final List<String> LOGINFO_ORDER = generateLogInfoOrder();

    private static final ThreadLocal<RequestLog> RLOG = ThreadLocal.withInitial(RequestLog::new);

    /**
     * Thou shalt not.
     */
    private RequestLogUtils() {
    }

    /**
     * Check if a stopwatch is started.
     *
     * @param caller  the caller to name this stopwatch with its class's simple name
     *
     * @return whether this stopwatch is started
     */

    public static boolean isStarted(Object caller) {
        return isStarted(caller.getClass().getSimpleName());
    }

    /**
     * Check if a stopwatch is started.
     *
     * @param timePhaseName  the name of this stopwatch
     *
     * @return whether this stopwatch is started
     */
    public static boolean isStarted(String timePhaseName) {
        RequestLog current = RLOG.get();
        TimedPhase timePhase = current.times.get(timePhaseName);
        return timePhase != null && timePhase.isStarted();
    }

    /**
     * Start timing the request.
     * Should be called exactly once, when a request is received by Fili
     */
    public static void startTimingRequest() {
        startTiming(TOTAL_TIMER);
    }

    /**
     * Start a stopwatch.
     * Time is accumulated if the stopwatch is already registered
     *
     * @param caller  the caller to name this stopwatch with its class's simple name
     */
    public static void startTiming(Object caller) {
        startTiming(caller.getClass().getSimpleName());
    }

    /**
     * Start a stopwatch.
     * Time is accumulated if the stopwatch is already registered
     *
     * @param timePhaseName  the name of this stopwatch
     */
    public static void startTiming(String timePhaseName) {
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
        timePhase.start();
    }

    /**
     * Stop the most recent stopwatch and start this one.
     * Time is accumulated if the stopwatch is already registered.
     *
     * @param nextPhase  the name of the stopwatch to be started
     */
    public static void switchTiming(String nextPhase) {
        stopMostRecentTimer();
        startTiming(nextPhase);
    }

    /**
     * Stops the request timer.
     * Should be called exactly once, just before Fili responds to the request
     */
    public static void stopTimingRequest() {
        stopTiming(TOTAL_TIMER);
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
        timePhase.stop();
    }

    /**
     * Stop the most recent stopwatch.
     */
    public static void stopMostRecentTimer() {
        try {
            stopTiming(RLOG.get().mostRecentTimer.name);
        } catch (NullPointerException ignored) {
            LOG.warn("Stopping timing failed because mostRecentTimer wasn't registered.");
        }
    }

    /**
     * Expose the request phases and their durations in nanoseconds.
     *
     * @return a mapping of phase -> duration (ns)
     */
    public static Map<String, Long> getDurations() {
        return RLOG.get().getDurations();
    }

    /**
     * Expose the request phases and their durations in milliseconds (including an entry for the longest druid query
     * that ran).
     *
     * @return a mapping of phase -> duration (ms)
     */
    public static Map<String, Float> getAggregateDurations() {
        return RLOG.get().getAggregateDurations(false);
    }

    /**
     * Record logging information in the logging context.
     *
     * @param logPhase  the name of the class destined to hold this logging information
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
        MDC.put(LOG_ID_KEY, newId);
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
        MDC.put(LOG_ID_KEY, current.logId);
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
                                        (e.getKey().equals(REQUEST_WORKFLOW_TIMER) && !e.getValue().isStarted()) ||
                                        (e.getKey().equals(RESPONSE_WORKFLOW_TIMER) && e.getValue().isStarted())
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
        record(new Durations(current.getAggregateDurations(true)));
        record(new Threads(current.threadIds));
        return LogFormatterProvider.getInstance().format(current.info);
    }

    protected static List<String> getLoginfoOrder() {
        return LOGINFO_ORDER;
    }

    /**
     * Get the logging blocks that need to be at the "head" of the log output.
     *
     * @return the blocks in order
     */
    private static List<String> generateLogInfoOrder() {
        String order = SYSTEM_CONFIG.getStringProperty(
                SYSTEM_CONFIG.getPackageVariableName("requestlog_loginfo_order"), ""
        );
        return generateLogInfoOrder(order);
    }

    /**
     * This is a silly method that is only exposed for testing.
     *
     * @param order The requested order of the initial blocks.
     * @return the blocks in order
     */
    private static List<String> generateLogInfoOrder(String order) {
        return Arrays.stream(order.replaceAll("\\s+", "").split(","))
                .filter(not(String::isEmpty))
                .map(name -> "com.yahoo.bard.webservice.logging.blocks." + name)
                .collect(Collectors.toList());
    }
}
