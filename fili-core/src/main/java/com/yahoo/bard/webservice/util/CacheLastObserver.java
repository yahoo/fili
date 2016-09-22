// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import rx.Observer;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thread-safe observable that caches the last message it received, and caches the last error (if any) it receives.
 *
 * @param <T>  The type of message this Observer caches
 */
public class CacheLastObserver<T> implements Observer<T> {

    private AtomicReference<T> lastMessageReceived = new AtomicReference<>();
    private AtomicReference<Throwable> error = new AtomicReference<>();

    @Override
    public void onCompleted() {
        //Intentionally left blank.
    }

    @Override
    public void onError(Throwable e) {
        error.set(e);
    }

    @Override
    public void onNext(T message) {
        lastMessageReceived.set(message);
    }

    /**
     * The last message received by this Observable, if any.
     *
     * @return the last message received by this Observable, if any
     */
    public Optional<T> getLastMessageReceived() {
        return Optional.ofNullable(lastMessageReceived.get());
    }

    /**
     * Returns the last error received by this Observable, if any.
     *
     * @return The last error received by this Observable, if any
     */
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error.get());
    }
}
