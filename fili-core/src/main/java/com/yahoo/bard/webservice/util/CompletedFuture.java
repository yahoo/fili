// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A future response which has already been completed and will return
 * as soon as it is called.
 *
 * @param <E>  The type to be returned by this future
 */
public class CompletedFuture<E> implements Future<E> {
    private final E item;
    private final Throwable throwable;

    /**
     * Creates a completed future with an item to return or
     * if a nonnull throwable is passed it it will throw an
     * execution exception.
     *
     * @param item  The item to return when called.
     * @param throwable  A non null throwable if an exception should be thrown.
     */
    public CompletedFuture(E item, Throwable throwable) {
        this.item = item;
        this.throwable = throwable;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
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
    public E get() throws ExecutionException {
        if (this.throwable != null) {
            throw new ExecutionException(throwable);
        } else {
            return item;
        }
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws ExecutionException {
        return get();
    }
}
