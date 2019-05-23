// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit

import spock.lang.Specification

class CallbackRateLimitRequestTokenSpec extends Specification {
    RateLimitCleanupOnRequestComplete callback
    RateLimitRequestToken token
    Runnable runnable

    def setup() {
        runnable = Mock(Runnable)

        callback = new RateLimitCleanupOnRequestComplete() {
            @Override
            void cleanup() {
                runnable.run()
            }
        }

        token = new CallbackRateLimitRequestToken(true, callback)
    }

    def "Callback runs on close"() {
        when:
        token.close()

        then:
        1 * runnable.run()
    }

    def "Doesn't close if already closed"() {
        when:
        token.close()
        token.close()

        then:
        1 * runnable.run()
    }

    def "If initialized unbound, behaves as if closed"() {
        setup:
        token = new CallbackRateLimitRequestToken(false, callback)

        when:
        token.close()

        then:
        0 * runnable.run()
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
