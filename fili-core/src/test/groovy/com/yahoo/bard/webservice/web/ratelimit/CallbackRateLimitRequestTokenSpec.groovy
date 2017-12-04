// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit

import spock.lang.Specification

class CallbackRateLimitRequestTokenSpec extends Specification {

    class Cell<T> {
        T data

        void set(T data) {
            this.data = data
        }

        T get() {
            return data
        }
    }

    Cell<Integer> cell
    RateLimitCleanupOnRequestComplete callback
    RateLimitRequestToken token

    def setup() {
        cell = new Cell<>()
        cell.set(1)

        callback = new RateLimitCleanupOnRequestComplete() {
            @Override
            void cleanup() {
                cell.set(cell.get() + 1)
            }
        }

        token = new CallbackRateLimitRequestToken(true, callback)
    }

    def "Callback runs on close"() {
        when:
        token.close()

        then:
        cell.get() == 2
    }

    def "Doesn't close if already closed"() {
        when:
        token.close()
        token.close()

        then:
        cell.get() == 2
    }

    def "If initialized unbound, behaves as if closed"() {
        setup:
        token = new CallbackRateLimitRequestToken(false, callback)

        when:
        token.close()

        then:
        cell.get() == 1
    }

    def "If token is initialized bound, it only unbinds when closed"() {
        expect:
        token.isBound() == true
        token.bind() == true

        when:
        token.close()

        then:
        token.isBound() == false
        token.bind() == false
    }

}
