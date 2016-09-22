// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static org.joda.time.DateTimeZone.UTC

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Test ZonedTimeGrain.
 */
class ZonedTimeGrainSpec extends Specification {

    static final String DENVER_NAME = "America/Denver"
    static final DateTimeZone DENVER_TZ = DateTimeZone.forID(DENVER_NAME)

    @Unroll
    def "test time zone granularity withZone turns #original into #expected"() {
        expect:
        expected == original.withZone(DENVER_TZ)

        where:
        original                      | expected
        new ZonedTimeGrain(DAY, UTC)  | new ZonedTimeGrain(DAY, DENVER_TZ)
        new ZonedTimeGrain(WEEK, UTC) | new ZonedTimeGrain(WEEK, DENVER_TZ)
    }

    def "Test zoneless time grain aligns with request time zone"() {
        setup:
        ZonedTimeGrain utcYearGrain = new ZonedTimeGrain(YEAR, UTC)
        ZonedTimeGrain denverYearGrain = new ZonedTimeGrain(YEAR, DENVER_TZ)

        DateTime denverYear = new DateTime("2015").withZoneRetainFields(DENVER_TZ)
        DateTime  utcYear = new DateTime("2015").withZoneRetainFields(DateTimeZone.UTC)
        DateTime denverYearInUTC = denverYear.withZone(DateTimeZone.UTC)

        expect:
        utcYearGrain.aligns(utcYear)
        denverYearGrain.aligns(denverYear)
        ! utcYearGrain.aligns(denverYear)
        ! utcYearGrain.aligns(denverYearInUTC)
        denverYearGrain.aligns(denverYearInUTC)
    }
}
