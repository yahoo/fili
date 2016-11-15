// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator

import spock.lang.Specification
import spock.lang.Unroll

public class SentinelSpec extends Specification {

    // The sole purpose of the sentinel is to have a lower precedence than everything else

    @Unroll
    def "sentinel must have lowest precedence"(Precedence other) {
        setup:
        def sentinel = new Sentinel()

        expect:
        other.greaterThan(sentinel.getPrecedence())

        where:
        other << Arrays.stream(Precedence.values()).filter({a -> a != Precedence.SENTINEL})
    }
}
