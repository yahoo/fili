// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.concurrent.CompletableFuture;

/**
 * A utility class to create futures which have already been completed
 * and will return as soon as it is called.
 *
 */
public class CompletedFuture {

    /**
     * Private constructor - all methods static.
     */
    private CompletedFuture() {
        //Intentionally left blank.
    }

    /**
     * Creates a completed {@link java.util.concurrent.Future} which will return this item.
     *
     * @param item  The item to return when called
     * @param <E>  The type to be returned by this future
     *
     * @return a completed {@link CompletableFuture} which will successfully return an item.
     */
    public static <E> CompletableFuture<E> returning(E item) {
        return CompletableFuture.completedFuture(item);
    }

    /**
     * Creates a completed {@link java.util.concurrent.Future} which will throw an
     * {@link java.util.concurrent.ExecutionException}.
     *
     * @param throwable  A throwable to be included in the {@link java.util.concurrent.ExecutionException}
     * @param <E>  The type to be returned by this future
     *
     * @return a completed {@link CompletableFuture} which will fail and throw an exception
     */
    public static <E> CompletableFuture<E> throwing(Throwable throwable) {
        CompletableFuture<E> completedFuture = CompletableFuture.supplyAsync(() -> null);
        completedFuture.completeExceptionally(throwable);
        return completedFuture;
    }
}
