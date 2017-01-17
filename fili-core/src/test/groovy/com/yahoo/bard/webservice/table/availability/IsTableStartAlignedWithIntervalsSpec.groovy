// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.table.PhysicalTable

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Days
import org.joda.time.Hours
import org.joda.time.Interval
import org.joda.time.Weeks

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class IsTableStartAlignedWithIntervalsSpec extends Specification {

    @Shared
    PhysicalTable tableUTC, tableCST, tableUTCHour, tableHalfHourly, tableHourlyAtHalfHour

    @Shared
    List<Interval> utcDays, cstDays, pacificDays, iranDays, utcHours, utcWeeks

    static DateTimeZone utc = DateTimeZone.UTC
    static DateTimeZone chicago = DateTimeZone.forID("America/Chicago")
    static DateTimeZone usPacific = DateTimeZone.forID("US/Pacific-New")
    static DateTimeZone iran = DateTimeZone.forID("Iran")

    static ZonedTimeGrain utcDay, utcHour, utcWeek, chicagoDay, pacificDay, iranDay

    def setupSpec() {
        utcDay = new ZonedTimeGrain(DAY, utc)
        utcHour = new ZonedTimeGrain(HOUR, utc)
        utcWeek = new ZonedTimeGrain(WEEK, utc)

        chicagoDay = new ZonedTimeGrain(DAY, chicago)
        pacificDay = new ZonedTimeGrain(DAY, usPacific)
        iranDay = new ZonedTimeGrain(DAY, iran)

        tableUTC = Mock(PhysicalTable)
        tableUTC.getTimeGrain() >> utcDay
        tableUTC.getTableAlignment() >> utcDay.roundFloor(new DateTime())

        tableUTCHour = Mock(PhysicalTable)
        tableUTCHour.getTimeGrain() >> utcHour
        tableUTCHour.getTableAlignment() >> utcDay.roundFloor(new DateTime())

        tableCST = Mock(PhysicalTable)
        tableCST.getTimeGrain() >> chicagoDay
        tableCST.getTableAlignment() >> chicagoDay.roundFloor(new DateTime())

        //utcDays, cstDays, pacificDays, indiaDays, utcHours, utcWeeks, utcMonths
        DateTime start = new DateTime(utc).withTimeAtStartOfDay()
        Interval interval = new Interval(start, Days.FOUR)
        utcDays = [interval]

        start = new DateTime(chicago).withTimeAtStartOfDay()
        interval = new Interval(start, Days.FOUR)
        cstDays = [interval]

        start = new DateTime(usPacific).withTimeAtStartOfDay()
        interval = new Interval(start, Days.FOUR)
        pacificDays = [interval]

        start = new DateTime(iran).withTimeAtStartOfDay()
        interval = new Interval(start, Days.FOUR)
        iranDays = [interval]

        start = new DateTime(utc).withTimeAtStartOfDay().plusHours(2)
        interval = new Interval(start, Hours.FOUR)
        utcHours = [interval]

        start = new DateTime(utc).withDayOfWeek(1).withTimeAtStartOfDay()
        interval = new Interval(start, Weeks.ONE)
        utcWeeks = [interval]
    }

    @Unroll
    def "Test whether table #table and intervals #intervals align: #aligns"() {
        given:
        IsTableStartAlignedWithIntervals predicate = new IsTableStartAlignedWithIntervals(intervals)

        expect:
        predicate.test(table) == aligns

        where:
        table                   | intervals     | aligns
        tableUTC                | utcDays       | true
        tableUTC                | cstDays       | false
        tableUTC                | pacificDays   | false
        tableUTC                | iranDays      | false
        tableUTC                | utcHours      | false
        tableUTC                | utcWeeks      | true
        tableCST                | utcDays       | false
        tableCST                | cstDays       | true
        tableCST                | pacificDays   | false
        tableCST                | iranDays      | false
        tableCST                | utcHours      | false
        tableCST                | utcWeeks      | false
        tableUTCHour            | utcDays       | true
        tableUTCHour            | cstDays       | true
        tableUTCHour            | pacificDays   | true
        tableUTCHour            | iranDays      | false
        tableUTCHour            | utcHours      | true
        tableUTCHour            | utcWeeks      | true
    }
}
