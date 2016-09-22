// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import rx.Observable
import spock.lang.Specification
import spock.lang.Unroll


class CacheLastObserverSpec extends Specification {

    @Unroll
    def "When sent messages #messages the Observable remembers #lastMessage"() {
        given: "The Observer under test"
        CacheLastObserver<Integer> observer = new CacheLastObserver<>()

        when: "We subscribe to an Observable that generates a bunch of messages"
        Observable.from(messages).subscribe(observer)

        then: "The observable remembers only the last message"
        if (messages) {
            observer.getLastMessageReceived().isPresent()
            observer.getLastMessageReceived().get() == lastMessage
        } else {
            !observer.getLastMessageReceived().isPresent()
        }

        and: "No errors were generated"
        !observer.getError().isPresent()

        where:
        messages  | _
        []        | _
        [1]       | _
        [1, 2, 3] | _
        lastMessage = messages ? messages[-1] : "nothing"
    }

    def "When an error is generated, the error is cached as is the last message"() {
        given: "The Observer under test"
        CacheLastObserver<Integer> observer = new CacheLastObserver<>()

        and: "The exception we'll be throwing"
        Exception exception = new RuntimeException("Test")

        when: "We subscribe to an Observable that generates a few messages and then an error"
        Observable.create {
            it.onNext(5)
            it.onNext(3)
            it.onError(exception)
        }
        .subscribe(observer)

        then: "The caching observer stores the last message received"
        observer.getLastMessageReceived().isPresent()
        observer.getLastMessageReceived().get() == 3

        and: "The caching observer stores the error"
        observer.getError().isPresent()
        observer.getError().get() == exception
    }
}
