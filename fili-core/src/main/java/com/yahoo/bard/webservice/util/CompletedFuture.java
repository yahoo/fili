// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.concurrent.CompletableFuture;

/**
 * A utility class to create futures which have already been completed and will return as soon as it is called.
 */
public class CompletedFuture {

    /**
     * Private constructor - all methods static.
     */
    private CompletedFuture() {
        // Intentionally left blank.
    }

    /**
     * Creates a completed {@link java.util.concurrent.Future} which will throw the given exception.
     *
     * @param throwable  A throwable to be thrown upon calling {@link java.util.concurrent.Future#get()} and related
     * methods.
     * @param <E>  The type to be returned by this future
     *
     * @return a completed {@link CompletableFuture} which will fail and throw an exception.
     */
    public static <E> CompletableFuture<E> throwing(Throwable throwable) {
        CompletableFuture<E> completedFuture = CompletableFuture.supplyAsync(() -> null);
        completedFuture.completeExceptionally(throwable);
        return completedFuture;
    }
}
