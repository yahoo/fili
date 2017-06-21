// Copyright 2017 Yahoo Inc.
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
    private Either<E, Throwable> result;

    /**
     * Creates a completed {@link Future} which will either return an item or throw
     * an {@link ExecutionException} with the throwable.
     *
     * @param result  Either a result to return a throwable to be included in the {@link ExecutionException}.
     */
    private CompletedFuture(Either<E, Throwable> result) {
        this.result = result;
    }

    /**
     * Creates a completed {@link Future} which will return this item.
     *
     * @param item  The item to return when called.
     * @param <E>  The type to be returned by this future.
     *
     * @return a completed {@link Future} which will successfully return an item.
     */
    public static <E> CompletedFuture<E> returning(E item) {
        return new CompletedFuture<>(Either.left(item));
    }

    /**
     * Creates a completed {@link Future} which will throw an {@link ExecutionException}.
     *
     * @param throwable  A throwable to be included in the {@link ExecutionException}.
     * @param <E>  The type to be returned by this future.
     *
     * @return a completed {@link Future} which will fail and throw an exception.
     */
    public static <E> CompletedFuture<E> throwing(Throwable throwable) {
        return new CompletedFuture<>(Either.right(throwable));
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
        if (result.isRight()) {
            throw new ExecutionException(result.getRight());
        }
        return result.getLeft();
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws ExecutionException {
        return get();
    }
}
