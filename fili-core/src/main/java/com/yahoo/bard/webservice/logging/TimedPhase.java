// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Represents a phase that is timed.
 * TimedPhase is used to associate a Timer located in the registry with the exact duration of such a phase for a
 * specific request. Times are in nanoseconds.
 * <p>
 * Note: This class is NOT thread-safe. Timers are intended to be started once by one thread, and stopped once by
 * one thread (though those threads are not necessarily the same).
 */
public class TimedPhase implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TimedPhase.class);

    private final String name;
    private long start;
    private long duration;

    /**
     * Constructor.
     *
     * @param name  Name of the phase
     */
    public TimedPhase(String name) {
        this.name = name;
    }

    /**
     * Start the phase.
     *
     * @return This phase after being started
     */
    public TimedPhase start() {
        if (isRunning()) {
            LOG.warn("Tried to start timer that is already running: {}", name);
        } else {
            start = System.nanoTime();
        }
        return this;
    }

    /**
     * Stop the phase.
     * <p>
     * This method just stops the timer. It does not register the time with the {@link RequestLog}. To register
     * the timer, invoke {@link TimedPhase#registerTime()}. To do both with a single method call, see
     * {@link TimedPhase#close()}
     *
     * @see TimedPhase#registerTime()
     * @see TimedPhase#close()
     */
    public void stop() {
        if (!isRunning()) {
            LOG.warn("Tried to stop timer that has not been started: {}", name);
            return;
        }
        duration += System.nanoTime() - start;
        start = 0;
    }

    /**
     * Registers the duration of this timer with the RequestLog.
     * <p>
     * It is highly recommended that you {@link TimedPhase#stop()}} the timer first. Otherwise, the timings may
     * be inaccurate. To both stop and register the timer at once see {@link TimedPhase#close}.
     *
     * @see TimedPhase#stop()
     * @see TimedPhase#close()
     */
    public void registerTime() {
        RequestLog.registerTime(this);
    }

    /**
     * Return the duration of the timer in nanoseconds.
     *
     * @return The duration of the timer in nanoseconds
     */
    public long getDuration() {
        if (isRunning()) {
            LOG.warn("Timer '{}' is still running. Timings may be incorrect.", getName());
        }
        return duration;
    }

    public String getName() {
        return name;
    }

    public TimeUnit getUnit() {
        return TimeUnit.NANOSECONDS;
    }

    public boolean isRunning() {
        return start != 0;
    }

    /**
     * Stops the timer, and registers the timer with the RequestLog.
     * <p>
     *  This is primarily meant to be used by the try-with-resources block, which both stops the timer and registers it
     *  with the RequestLog, though it can of course be called manually as well. If you want to stop the timer, but
     *  don't want to register the timer just yet, then see {@link TimedPhase#stop}.
     *
     * @see TimedPhase#stop()
     * @see TimedPhase#registerTime()
     */
    @Override
    public void close() {
        stop();
        registerTime();
    }
}
