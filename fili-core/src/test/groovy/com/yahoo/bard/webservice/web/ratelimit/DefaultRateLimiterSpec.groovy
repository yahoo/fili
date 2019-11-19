// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit

import com.yahoo.bard.webservice.web.filters.RateLimitFilterSpec

import spock.lang.Specification

import java.security.Principal

import javax.ws.rs.HttpMethod
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.SecurityContext

class DefaultRateLimiterSpec extends Specification {

    static String BYPASS_HEADER_NAME
    static String BYPASS_HEADER_VALUE

    static String CLIENT_HEADER_NAME
    static String CLIENT_HEADER_UI
    static String CLIENT_HEADER_USER

    static String REFERER_HEADER_NAME
    static String REFERER_HEADER_VALUE

    final static String USERNAME = "user"

    DefaultRateLimiter rateLimiter
    Principal user
    MultivaluedMap<String, String> headers
    SecurityContext securityContext
    String requestMethod
    ContainerRequestContext request

    def setupSpec() {
        RateLimitFilterSpec.setDefaults()

        BYPASS_HEADER_NAME = "bard-testing"
        BYPASS_HEADER_VALUE = "###BYPASS###"

        CLIENT_HEADER_NAME = "clientid"
        CLIENT_HEADER_UI = "UI"
        CLIENT_HEADER_USER = "USER"

        REFERER_HEADER_NAME = "referer"
        REFERER_HEADER_VALUE = "test"
    }

    def setup() {
        user = Mock(Principal)
        user.getName() >> USERNAME

        rateLimiter = new DefaultRateLimiter()
        // reset metrics to 0
        rateLimiter.with {
            [requestGlobalCounter, usersCounter].each { it.dec(it.count) }
            [requestBypassMeter, requestUiMeter, requestUserMeter, rejectUiMeter, rejectUserMeter].each {
                it.mark(-it.count)
            }
        }

        headers = new MultivaluedHashMap<>()
        securityContext = Mock(SecurityContext)
        request = Mock(ContainerRequestContext)
        headers.add(CLIENT_HEADER_NAME, CLIENT_HEADER_USER)
        requestMethod = HttpMethod.GET
        securityContext.getUserPrincipal() >> { return user}
        request.getHeaders() >> {return headers }
        request.getMethod() >> { return requestMethod }
        request.getSecurityContext() >> { return securityContext }
    }

    def "bypass request"() {
        setup:
        headers.remove(CLIENT_HEADER_NAME)
        headers.add(BYPASS_HEADER_NAME, BYPASS_HEADER_VALUE)

        when:
        RateLimitRequestToken token = rateLimiter.getToken(request)
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

    def "CORS preflight request"() {
        setup:
        headers.remove(CLIENT_HEADER_NAME)
        requestMethod = HttpMethod.OPTIONS
        securityContext = null

        when:
        RateLimitRequestToken token = rateLimiter.getToken(request)
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

    def "user request"() {
        when:
        RateLimitRequestToken token = rateLimiter.getToken(request)
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
        setup:
        headers.remove(CLIENT_HEADER_NAME)
        headers.add(CLIENT_HEADER_NAME, CLIENT_HEADER_UI)
        headers.add(REFERER_HEADER_NAME, REFERER_HEADER_VALUE)

        when:
        RateLimitRequestToken token = rateLimiter.getToken(request)
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

    def "username of request is stored while request is open, doesn't get removed on all requests closed"() {
        expect:
        rateLimiter.userCounts.containsKey(USERNAME) == false

        when:
        RateLimitRequestToken token = rateLimiter.getToken(request)

        then:
        rateLimiter.userCounts.containsKey(USERNAME) == true

        when:
        token.close()

        then:
        rateLimiter.userCounts.containsKey(USERNAME) == true

    }

    def "Null user request"() {
        setup:
        user = null

        when: "We have the global limit of tokens for a null user"
        List<RateLimitRequestToken> tokens = (1..RateLimitFilterSpec.LIMIT_GLOBAL).collect { rateLimiter.getToken(request) }

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
        RateLimitRequestToken token = rateLimiter.getToken(request)

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
        RateLimitRequestToken token = rateLimiter.getToken(request)

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
        RateLimitRequestToken token = rateLimiter.getToken(request)
        rateLimiter.userCounts.get(USERNAME).decrementAndGet()
        token.close()

        then:
        IllegalStateException e = thrown()
        e.message == "Lost user count"
        rateLimiter.requestGlobalCounter.count == 1
    }

    def "Lose global counts"() {
        when:
        RateLimitRequestToken token = rateLimiter.getToken(request)
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
