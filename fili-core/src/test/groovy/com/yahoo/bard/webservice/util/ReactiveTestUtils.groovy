// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import rx.Subscriber
import rx.observers.TestSubscriber
import rx.Observable
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * A collection of generic utility functions to aid in testing reactive code.
 */
class ReactiveTestUtils {

    /**
     * Subscribes the specified number of {@link TestSubscriber} instances to the passed in `Observable` and returns
     * the subscribers.
     *
     * @param observable  The observable to have subscribers subscribed to
     * @param numObservers  The number of observers to subscribe
     *
     * @return The list of subscribers that were subscribed to the observable
     */
    static <T> List<Subscriber<T>> subscribeObservers (Observable<T> observable, int numObservers) {
        List<Subscriber<T>> subscribers = (1..numObservers).collect {new TestSubscriber<>()}
        subscribers.every {observable.subscribe(it)}
        return subscribers
    }

    /**
     * Blocks until the given subscriber receives an {@code onCompleted} message, or a timeout expires.
     * <p>
     * The assertion will fail if the timeout expires before an {@code onCompleted} message is received, or an
     * {@code onError} message is received within the timeout.
     *
     * @param subscriber  The subscriber that should be checked for eventual completion
     * @param timeout  The number of time units to wait (defaults to 5)
     * @param timeoutUnit  The units to use for the timeout (defaults to {@link TimeUnit#SECONDS}.
     *
     * @throws AssertionError if the timeout expires without an {@code onCompleted} message, or an {@code onError}
     * message is received within the timeout
     */
    static void assertCompletedWithoutError(
            TestSubscriber subscriber,
            int timeout = 5,
            TimeUnit timeoutUnit = TimeUnit.SECONDS
    ) {
        subscriber.awaitTerminalEvent(timeout, timeoutUnit)
        subscriber.assertNoErrors()
        subscriber.assertCompleted()
    }
}
