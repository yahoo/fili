// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

/**
 * Defines a task that is scheduled to run at a given time, potentially periodically.
 *
 * @param <V>  The type of the result returned by the task associated with this loader.
 */
public abstract class LoadTask<V> implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LoadTask.class);
    public static final String LOAD_TASK_ERROR_FORMAT = "Exception while running %s: %s";

    protected final String loaderName;
    protected final long delay;
    protected final long period;
    protected final boolean isPeriodic;
    protected ScheduledFuture<?> future;

    private boolean failed = false;

    /**
     * Creates a one-off loader.
     *
     * @param loaderName  The name of the loader.
     * @param delay  The initial delay of this loader in milliseconds.
     */
    public LoadTask(String loaderName, long delay) {
        this(loaderName, delay, 0);
    }

    /**
     * Creates a periodic loader.
     *
     * @param loaderName  The name of the loader.
     * @param delay  The initial delay of this loader in milliseconds.
     * @param period  The period of this loader in milliseconds. Zero period corresponds to a one-off loader.
     */
    public LoadTask(String loaderName, long delay, long period) {
        this.loaderName = loaderName;
        this.delay = delay;
        this.period = period;
        this.isPeriodic = period > 0;
    }

    /**
     * Internal implementation of {@link LoadTask#run()} task that will be used so that {@link LoadTask#run()} can
     * add mandatory exception handling.
     */
    public abstract void runInner();

    @Override
    public final void run() {
        try {
            runInner();
        } catch (RuntimeException t) {
            LOG.error(String.format(LOAD_TASK_ERROR_FORMAT, getName(), t.getMessage()), t);
            failed = true;
            throw t;
        }
    };

    /**
     * Return the name of this loader.
     *
     * @return A String representing the name of the loader.
     */
    public String getName() {
        return loaderName;
    }

    @Override
    public String toString() {
        return Objects.toString(getName());
    }

    /**
     * Return whether this loader is periodic or one-off.
     *
     * @return Returns true if this loader is periodic.
     */
    public boolean isPeriodic() {
        return isPeriodic;
    }

    /**
     * Get the defined delay of this loader in milliseconds.
     *
     * @return The delay in milliseconds.
     */
    public long getDefinedDelay() {
        return delay;
    }

    /**
     * Get the defined period of this loader in milliseconds.
     *
     * @return The period in milliseconds. It returns zero if this is a one-off loader.
     */
    public long getDefinedPeriod() {
        return period;
    }

    /**
     * Get the future associated with the execution of this loader.
     * If the loader has not been scheduled yet via the {@link TaskScheduler} this method will return an empty Optional.
     *
     * @return An optional containing the future of this loader after it is scheduled.
     */
    public synchronized Optional<ScheduledFuture<?>> getFuture() {
        return Optional.ofNullable(future);
    }

    /**
     * Set the future associated with this loader.
     * Normally should be set by the task scheduler in the same package.
     *
     * @param future  The future to associate
     */
    synchronized void setFuture(ScheduledFuture<?> future) {
        this.future = future;
    }

    /**
     * Get a default callback for an http error.
     *
     * @return A newly created http error callback object.
     */
    protected HttpErrorCallback getErrorCallback() {
        return new TaskHttpErrorCallback();
    }

    /**
     * Get a default callback to use when a failure occurs.
     *
     * @return A newly created failure callback object.
     */
    protected FailureCallback getFailureCallback() {
        return new TaskFailureCallback();
    }

    /**
     * A basic nested class dealing with http errors that can be re-used by classes extending {@link LoadTask}.
     */
    protected class TaskHttpErrorCallback implements HttpErrorCallback {
        @Override
        public void invoke(int statusCode, String reason, String responseBody) {
            LOG.error(
                    "{}: Druid HTTP call back error {}, Cause: {}, Response body: {}",
                    getName(),
                    statusCode,
                    reason,
                    responseBody
            );
        }
    }

    /**
     * A basic nested class dealing with failures that can be re-used by classes extending {@link LoadTask}.
     */
    protected class TaskFailureCallback implements FailureCallback {
        @Override
        public void invoke(Throwable error) {
            LOG.error("{}: Async request to druid failed:", getName(), error);
        }
    }

    public boolean isFailed() {
        return failed;
    }
}
