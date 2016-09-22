// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MINUTE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.QUARTER
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTime

import spock.lang.Specification
import spock.lang.Unroll

class DefaultTimeGrainSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Unroll
    def "#timeGrain serializes to #expectedJson with default timezone (UTC)"() {
        expect:
        GroovyTestUtils.compareJson(MAPPER.writeValueAsString(timeGrain), expectedJson)

        where:
        timeGrain | expectedJson
        MONTH   | '{"type":"period","period":"P1M"}'
        DAY     | '{"type":"period","period":"P1D"}'
        YEAR    | '{"type":"period","period":"P1Y"}'
        WEEK    | '{"type":"period","period":"P1W"}'
        HOUR    | '{"type":"period","period":"PT1H"}'
        MINUTE  | '{"type":"period","period":"PT1M"}'
        QUARTER | '{"type":"period","period":"P3M"}'
    }

    @Unroll
    def "#timeGrain rounds to #expected"() {
        setup:
        DateTime dateTime = new DateTime("2015-05-02T12:37:22.002")

        expect:
        timeGrain.roundFloor(dateTime) == new DateTime(expected)

        where:
        timeGrain | expected
        MONTH     | "2015-05"
        DAY       | "2015-05-02"
        YEAR      | "2015"
        WEEK      | "2015-04-27"
        HOUR      | "2015-05-02T12"
        MINUTE    | "2015-05-02T12:37"
        QUARTER   | "2015-04"
    }

    @Unroll
    def "#fromDate rounds to #expected"() {
        setup:
        DateTime dateTime = new DateTime(fromDate)

        expect:
        QUARTER.roundFloor(dateTime) == new DateTime(expected)

        where:
        fromDate                      | expected
        "2015-05-02T12:37:22.002"     | "2015-04"
        "2015-04-02T12:37:22.002"     | "2015-04"
        "2015-03-02T12:37:22.002"     | "2015-01"
    }
}
