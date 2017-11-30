package com.yahoo.bard.webservice.web.ratelimit

//// Copyright 2017 Yahoo Inc.
//// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
//package com.yahoo.bard.webservice.web
//
//import com.codahale.metrics.Counter
//import com.codahale.metrics.Meter
//
//import spock.lang.Specification
//
//import java.security.Principal
//import java.util.concurrent.atomic.AtomicInteger
//
//class com.yahoo.bard.webservice.web.ratelimit.OutstandingRateLimitedRequestTokenSpec extends Specification{
//
//    Principal user
//    AtomicInteger globalCount
//    AtomicInteger userCount
//    Meter requestMeter
//    Meter rejectMeter
//    Counter requestGlobalCounter
//
//    def setup() {
//        user = Mock(Principal)
//        user.getName() >> "user"
//
//        userCount = new AtomicInteger()
//        globalCount = new AtomicInteger()
//
//        requestMeter = new Meter()
//        rejectMeter = new Meter()
//        requestGlobalCounter = new Counter()
//    }
//
//    def "Test creating token below request limit increments relevant counters, and decrements on close"() {
//        setup:
//        int requestLimit = 2
//        int globalRequestLimit = 3
//        AtomicInteger userCount = new AtomicInteger()
//
//        when: "create a new token"
//        RateLimitRequestToken token = new DefaultRateLimiter.OutstandingRateLimitRequestToken(user, false)
//
//        then: "counters have incremented"
//        token.isBound() == true
//        userCount.get() == 1
//        globalCount.get() == 1
//        requestMeter.getCount() == 1
//        rejectMeter.getCount() == 0
//        requestGlobalCounter.getCount() == 1
//
//        when: "close the token"
//        token.close()
//
//        then: "user counters drop, global counters maintain"
//        token.isBound() == false
//        userCount.get() == 0
//        globalCount.get() == 0
//        requestMeter.getCount() == 1
//        rejectMeter.getCount() == 0
//        requestGlobalCounter.getCount() == 1
//    }
//
//    def "Test creating token at request limit rejects the request"() {
//        setup:
//        int requestLimit = 0
//        int globalRequestLimit = 3
//        AtomicInteger userCount = new AtomicInteger()
//
//        when: "create a new token"
//        RateLimitRequestToken token = new DefaultRateLimiter.OutstandingRateLimitRequestToken(user, false)
//
//        then: "request was rejected, and not bound"
//        token.isBound() == false
//        userCount.get() == 0
//        globalCount.get() == 0
//        requestMeter.getCount() == 0
//        rejectMeter.getCount() == 1
//        requestGlobalCounter.getCount() == 0
//    }
//
//    def "Test hitting global limit rejects future requests"() {
//        setup: "create 3 tokens for user1"
//        int requestLimit = 3
//        int globalRequestLimit = 3
//        AtomicInteger userCount = new AtomicInteger()
//        Principal user2 = Mock(Principal)
//        AtomicInteger user2Count = new AtomicInteger()
//        user2.getName() >> "user2"
//
//        // launch three requests; this will bring us to the global limit
//        (1..3).each {
//            new DefaultRateLimiter.OutstandingRateLimitRequestToken(user, false)
//        }
//
//        when: "create a new token (launch a new request) for user2"
//        RateLimitRequestToken user2Token = new DefaultRateLimiter.OutstandingRateLimitRequestToken(user2, false)
//
//        then: "user2 request was rejected, user1 requests are successful"
//        user2Token.isBound() == false
//        user2Count.get() == 0
//        userCount.get() == 3
//        globalCount.get() == 3
//        requestMeter.getCount() == 3
//        rejectMeter.getCount() == 1
//        requestGlobalCounter.getCount() == 3
//    }
//}
