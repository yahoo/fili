// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests on the default methods in Zoneless Time Grain
 */
class ZonelessTimeGrainSpec extends Specification {

    static final String DENVER_NAME = "America/Denver"
    static final DateTimeZone DENVER_TZ = DateTimeZone.forID(DENVER_NAME)

    def "Test zoneless time grain aligns with request time zone"() {
        setup:
        DateTime denverYear = new DateTime("2015").withZoneRetainFields(DENVER_TZ)
        DateTime utcYear = new DateTime("2015").withZoneRetainFields(DateTimeZone.UTC)
        DateTime denverYearInUTC = denverYear.withZone(DateTimeZone.UTC)

        expect:
        YEAR.aligns(utcYear)
        YEAR.aligns(denverYear)
        ! YEAR.aligns(denverYearInUTC)
    }

    @Unroll
    def "test time zone granularity withZone turns #original into #expected"() {
        expect:
        expected == original.buildZonedTimeGrain(DENVER_TZ)

        where:
        original                     | expected
        DAY                          | new ZonedTimeGrain(DAY, DENVER_TZ)
        WEEK                         | new ZonedTimeGrain(WEEK, DENVER_TZ)
    }
}
