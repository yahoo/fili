// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.ratelimit

import com.codahale.metrics.Meter

import spock.lang.Specification

class BypassRateLimitRequestTokenSpec extends Specification {

    Meter bypassMeter
    RateLimitRequestToken token

    def setup() {
        bypassMeter = new Meter()
        token = new BypassRateLimitRequestToken()
    }

    def "Bypass token is always bound"() {
        expect:
        token.isBound() == true

        when: "close the token"
        token.close()

        then:
        token.isBound() == true

        when: "token was orphaned"
        token.finalize()

        then:
        token.isBound() == true
    }
}
