// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Defines a task that is scheduled to run at a given time, periodically or in a one-off fashion.
 *
 * @param <V>  The result type returned by the get method of the Future that is associated with this task.
 */
public class SchedulableTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {
    protected final String name;
    private final RunnableScheduledFuture<V> innerTask;

    /**
     * Creates a task that can be scheduled with {@link TaskScheduler}.
     *
     * @param runnable  The user-defined task to execute.
     * @param result  The result to return upon success. Use null if the task does not return a result along with
     * Void as generic object type.
     * @param innerTask  The inner task instantiated internally by the executor that we decorate with this class.
     */
    public SchedulableTask(Runnable runnable, V result, RunnableScheduledFuture<V> innerTask) {
        super(runnable, result);
        this.innerTask = innerTask;
        this.name = runnable.toString();
    }

    /**
     * Creates a task that can be scheduled with {@link TaskScheduler}.
     *
     * @param callable  The user-defined task to execute.
     * @param innerTask  The inner task instantiated internally by the executor that we decorate with this class.
     */
    public SchedulableTask(Callable<V> callable, RunnableScheduledFuture<V> innerTask) {
        super(callable);
        this.innerTask = innerTask;
        this.name = callable.toString();
    }

    @Override
    public void run() {
        if (isPeriodic()) {
            innerTask.run();
        } else {
            // Decoration of tasks doesn't play well with one-off tasks. Executing without using the innerTask.
            super.run();
        }
    }

    @Override
    public boolean isPeriodic() {
        return innerTask.isPeriodic();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return innerTask.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed task) {
        return innerTask.compareTo(task);
    }
}
