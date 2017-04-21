// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future response which cannot be completed and will throw
 * an execution exception upon {@link Future#get()}.
 * @param <E>  The type to be returned by the future.
 */
public class FailedFuture<E> implements Future<E> {
    private final String reason;

    /**
     * Construct a future which is already known to have failed.
     *
     * @param reason  The reason the future could not be completed.
     */
    public FailedFuture(String reason) {
        this.reason = reason;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        throw new ExecutionException(new RuntimeException(reason));
    }

    @Override
    public E get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }
}
