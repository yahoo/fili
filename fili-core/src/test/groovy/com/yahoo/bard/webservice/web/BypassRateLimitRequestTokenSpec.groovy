// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web

import com.codahale.metrics.Meter

import spock.lang.Specification

class BypassRateLimitRequestTokenSpec extends Specification {

    Meter bypassMeter

    def setup() {
        bypassMeter = new Meter()
    }

    def "Creating bypass token increments bypass meter"() {
        when: "create bypass token"
        new BypassRateLimitRequestToken(bypassMeter)

        then: "Meter incremented"
        bypassMeter.getCount() == 1
    }

    def "Bypass token is always bound"() {
        when: "create bypass token"
        RateLimitRequestToken token = new BypassRateLimitRequestToken(bypassMeter)

        then:
        token.isBound() == true

        when: "close the token"
        token.close()

        then:
        token.isBound() == true
    }
}
