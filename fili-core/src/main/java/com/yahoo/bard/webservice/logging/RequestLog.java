// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import static com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl.DRUID_QUERY_ALL_TIMER;
import static com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl.DRUID_QUERY_MAX_TIMER;
import static com.yahoo.bard.webservice.druid.client.impl.AsyncDruidWebServiceImpl.DRUID_QUERY_TIMER;

import com.yahoo.bard.webservice.application.MetricRegistryFactory;
import com.yahoo.bard.webservice.logging.blocks.Durations;
import com.yahoo.bard.webservice.logging.blocks.Preface;
import com.yahoo.bard.webservice.logging.blocks.Threads;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import com.codahale.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * The RequestLog holds data that we would like to log about the request. In particular, various timings are
 * accumulated here.
 */
public class RequestLog {
    private static final MetricRegistry REGISTRY = MetricRegistryFactory.getRegistry();
    private static final Logger LOG = LoggerFactory.getLogger(RequestLog.class);
    private static final long MS_PER_NS = 1000000;
    protected static final String LOG_ID_KEY = "logid";

    protected String logId;
    protected LogBlock info;
    protected TimedPhase mostRecentTimer;
    protected final Map<String, TimedPhase> times;
    protected final Set<String> threadIds;

    /**
     * This class has only static methods and is not supposed to be directly instantiated.
     */
    protected RequestLog() {
        logId = null;
        info = null;
        mostRecentTimer = null;
        times = new LinkedHashMap<>();
        threadIds = new LinkedHashSet<>();
        MDC.remove(LOG_ID_KEY);
        init();
    }

    /**
     * Copy constructor is also private.
     *
     * @param  rl request log object to copy from
     */
    protected RequestLog(RequestLog rl) {
        logId = rl.logId;
        info = rl.info;
        mostRecentTimer = rl.mostRecentTimer;
        times = new LinkedHashMap<>(rl.times);
        threadIds = new LinkedHashSet<>(rl.threadIds);
        MDC.put(LOG_ID_KEY, logId);
    }

    /**
     * Resets the contents of a request log at the calling thread.
     */
    protected void clear() {
        logId = null;
        info = null;
        mostRecentTimer = null;
        times.clear();
        threadIds.clear();
        MDC.remove(LOG_ID_KEY);
    }

    /**
     * Copys the data from source into the current request log
     * @param source the request log to copy from
     */
    protected void restoreFrom(RequestLog source) {
        clear();
        logId = source.logId;
        info = source.info;
        mostRecentTimer = source.mostRecentTimer;
        times.putAll(source.times);
        threadIds.addAll(source.threadIds);
        threadIds.add(Thread.currentThread().getName());

    }

    /**
     * Creates a new and empty request log at the calling thread.
     */
    protected void init() {
        logId = UUID.randomUUID().toString();
        info = new LogBlock(logId);
        // Trick to place Durations and Threads in front of the Json while keep using a LinkedHashMap.
        // The actual entries will be replaced later when export is called but the initial order will be respected
        info.add(Durations.class);
        info.add(Threads.class);
        info.add(Preface.class);
        RequestLogUtils.getLoginfoOrder().stream().filter(entry -> !Objects.equals(entry, "Epilogue")).forEachOrdered(
                entry -> {
                    try {
                        @SuppressWarnings("unchecked")
                        Class<LogInfo> cls = (Class<LogInfo>) Class.forName(entry);
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
        MDC.put(LOG_ID_KEY, logId);
    }

    /**
     * Retrieve a {@link TimedPhase} if it exists
     *
     * @param phaseName the name of the timed phase
     * @return the phase
     */
    protected TimedPhase getPhase(String phaseName) {
        return times.get(phaseName);
    }

    /**
     * Add a timed phase to the request log
     *
     * @param phase the phase to be timed
     */
    protected void putPhase(TimedPhase phase) {
        times.put(phase.name, phase);
    }

    /**
     * Adds the durations in milliseconds of all the recorded timed phases to a map.
     *
     * @return the map containing all the recorded times per phase in nanoseconds
     */
    protected Map<String, Long> getDurations() {
        return times.values()
                .stream()
                .peek(phase -> {
                    if (!phase.isRunning()) {
                        return;
                    }
                    LOG.warn("Exporting duration while timer is running. Measurement might be wrong: {}", phase.name);
                })
                .collect(Collectors.toMap(phase -> phase.name, phase -> phase.duration));
    }

    /**
     * Adds the durations in milliseconds of all the recorded timed phases to a map.
     *
     * @param updateTimers a flag indicating whether or not the metrics registry should be updated
     * @return the map containing all the recorded times per phase in milliseconds
     */
    protected Map<String, Float> getAggregateDurations(boolean updateTimers) {
        Map<String, Long> durations = getDurations();

        OptionalLong max = durations.entrySet()
                .stream()
                .filter(e -> e.getKey().contains(DRUID_QUERY_TIMER))
                .mapToLong(Map.Entry::getValue)
                .peek(v -> REGISTRY.timer(DRUID_QUERY_ALL_TIMER).update(v, TimeUnit.NANOSECONDS))
                .max();

        if (max.isPresent()) {
            if (updateTimers) {
                REGISTRY.timer(DRUID_QUERY_MAX_TIMER).update(max.getAsLong(), TimeUnit.NANOSECONDS);
            }
            durations.put(DRUID_QUERY_MAX_TIMER, max.getAsLong());
        }

        return durations.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> (float) e.getValue() / MS_PER_NS));
    }
}
