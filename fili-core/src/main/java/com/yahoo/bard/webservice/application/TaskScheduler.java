// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Defines a scheduler that runs tasks that load data from Druid periodically or in one-off fashion.
 */
public class TaskScheduler extends ScheduledThreadPoolExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

    /**
     * Creates a new scheduler for loaders with the given thread pool size.
     *
     * @param poolSize  The number of threads available in the pool of this scheduler.
     */
    public TaskScheduler(int poolSize) {
        super(poolSize);
        super.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        super.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        LOG.info("Started task scheduler with thread pool size: {}", poolSize);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        return new SchedulableTask<>(runnable, null, task);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
        return new SchedulableTask<>(callable, task);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down task scheduler with thread pool size: {}", getCorePoolSize());
        super.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        LOG.info("Immediately shutting down task scheduler with thread pool size: {}", getCorePoolSize());
        return super.shutdownNow();
    }
}
