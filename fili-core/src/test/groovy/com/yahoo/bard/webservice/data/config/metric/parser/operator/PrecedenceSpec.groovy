// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator

import spock.lang.Specification

public class PrecedenceSpec extends Specification {
    def "the greaterThan operator should work"() {
        expect:
        !Precedence.SENTINEL.greaterThan(Precedence.SENTINEL)
        !Precedence.SENTINEL.greaterThan(Precedence.ADD_SUB)
        Precedence.ADD_SUB.greaterThan(Precedence.SENTINEL)
    }
}
