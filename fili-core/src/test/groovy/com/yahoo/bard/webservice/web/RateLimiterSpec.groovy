// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.yahoo.bard.webservice.web.RateLimiter.RequestToken
import com.yahoo.bard.webservice.web.RateLimiter.RequestType
import com.yahoo.bard.webservice.web.filters.RateLimitFilterSpec

import spock.lang.Specification

import java.security.Principal

class RateLimiterSpec extends Specification {

    RateLimiter rateLimiter
    Principal user

    def setupSpec() {
        RateLimitFilterSpec.setDefaults()
    }

    def setup() {
        rateLimiter = new RateLimiter()
        user = Mock(Principal)
        user.getName() >> { return "user" }

        // reset metrics to 0
        rateLimiter.with {
            [requestGlobalCounter, usersCounter].each { it.dec(it.count) }
            [requestBypassMeter, requestUiMeter, requestUserMeter, rejectUiMeter, rejectUserMeter].each {
                it.mark(-it.count)
            }
        }
    }

    def "user request"() {
        when:
        RateLimiter.RequestToken token = rateLimiter.getToken(RequestType.USER, user)
        token.close()

        then:
        rateLimiter.requestGlobalCounter.count == 1
        rateLimiter.usersCounter.count == 1
        rateLimiter.requestBypassMeter.count == 0
        rateLimiter.requestUiMeter.count == 0
        rateLimiter.requestUserMeter.count == 1
        rateLimiter.rejectUiMeter.count == 0
        rateLimiter.rejectUserMeter.count == 0
    }

    def "ui request"() {
        when:
        RateLimiter.RequestToken token = rateLimiter.getToken(RequestType.UI, user)
        token.close()

        then:
        rateLimiter.requestGlobalCounter.count == 1
        rateLimiter.usersCounter.count == 1
        rateLimiter.requestBypassMeter.count == 0
        rateLimiter.requestUiMeter.count == 1
        rateLimiter.requestUserMeter.count == 0
        rateLimiter.rejectUiMeter.count == 0
        rateLimiter.rejectUserMeter.count == 0
    }

    def "bypass request"() {
        when:
        RateLimiter.RequestToken token = rateLimiter.getToken(RequestType.BYPASS, user)
        token.close()

        then:
        rateLimiter.requestGlobalCounter.count == 0
        rateLimiter.usersCounter.count == 0
        rateLimiter.requestBypassMeter.count == 1
        rateLimiter.requestUiMeter.count == 0
        rateLimiter.requestUserMeter.count == 0
        rateLimiter.rejectUiMeter.count == 0
        rateLimiter.rejectUserMeter.count == 0
    }


    def "Null user request"() {
        when: "We have the global limit of tokens for a null user"
        List<RequestToken> tokens = (1..RateLimitFilterSpec.LIMIT_GLOBAL).collect { rateLimiter.getToken(RequestType.USER,  null ) }

        and: "They have all been closed:"
        tokens.each { token -> token.close() }

        then: "The global counter has counted the per-user limit"
        rateLimiter.requestGlobalCounter.count == RateLimitFilterSpec.LIMIT_PER_USER

        and: "We have seen only 1 user"
        rateLimiter.usersCounter.count == 1

        and: "We have not allowed any bypass requests"
        rateLimiter.requestBypassMeter.count == 0

        and: "We have not allowed any UI requests"
        rateLimiter.requestUiMeter.count == 0

        and: "We have allowed the per-user limit of user requests"
        rateLimiter.requestUserMeter.count == RateLimitFilterSpec.LIMIT_PER_USER

        and: "We have not rejected any UI requests"
        rateLimiter.rejectUiMeter.count == 0

        and: "We have rejected the global limit of requests except for the per-user limit that we had allowed"
        rateLimiter.rejectUserMeter.count == RateLimitFilterSpec.LIMIT_GLOBAL-RateLimitFilterSpec.LIMIT_PER_USER
    }

    def "OutstandingRequestToken closed"() {
        when:
        RateLimiter.RequestToken token = rateLimiter.getToken(RequestType.USER, user)

        then:
        rateLimiter.globalCount.get() == 1
        rateLimiter.requestGlobalCounter.count == 1
        rateLimiter.usersCounter.count == 1

        when:
        token.close()

        then:
        rateLimiter.globalCount.get() == 0

        when:
        // closing again should have no affect
        token.close()

        then:
        rateLimiter.globalCount.get() == 0
        rateLimiter.requestGlobalCounter.count == 1
        rateLimiter.requestBypassMeter.count == 0
        rateLimiter.requestUiMeter.count == 0
        rateLimiter.requestUserMeter.count == 1
        rateLimiter.rejectUiMeter.count == 0
        rateLimiter.rejectUserMeter.count == 0
    }

    def "OutstandingRequestToken orphan closed"() {
        when:
        RateLimiter.RequestToken token = rateLimiter.getToken(RequestType.USER, user)

        then:
        rateLimiter.globalCount.get() == 1
        rateLimiter.requestGlobalCounter.count == 1

        when:
        // simulate GC
        token.finalize()

        then:
        rateLimiter.globalCount.get() == 0
        rateLimiter.requestGlobalCounter.count == 1
        rateLimiter.requestBypassMeter.count == 0
        rateLimiter.requestUiMeter.count == 0
        rateLimiter.requestUserMeter.count == 1
        rateLimiter.rejectUiMeter.count == 0
        rateLimiter.rejectUserMeter.count == 0
    }

    def "Lose user counts"() {
        when:
        RateLimiter.OutstandingRequestToken token = rateLimiter.getToken(RequestType.USER, user)
        token.count.decrementAndGet()
        token.close()

        then:
        IllegalStateException e = thrown()
        e.message == "Lost user count"
        rateLimiter.requestGlobalCounter.count == 1
    }

    def "Lose global counts"() {
        when:
        RateLimiter.RequestToken token = rateLimiter.getToken(RequestType.USER, user)
        rateLimiter.globalCount.decrementAndGet()
        token.close()

        then:
        rateLimiter.globalCount.get() == 0
        rateLimiter.requestGlobalCounter.count == 1
        rateLimiter.requestBypassMeter.count == 0
        rateLimiter.requestUiMeter.count == 0
        rateLimiter.requestUserMeter.count == 1
        rateLimiter.rejectUiMeter.count == 0
        rateLimiter.rejectUserMeter.count == 0
    }
}
