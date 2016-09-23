// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter

import org.joda.time.DateTime
import org.joda.time.Hours
import org.joda.time.Interval
import org.joda.time.Weeks

import spock.lang.Specification

class AllGranularitySpec extends Specification {

    AllGranularity allGranularity = AllGranularity.INSTANCE

    def "All granularity serializes to 'all'"() {
        setup:
        ObjectWriter writer = new ObjectMapper().writer()

        expect:
        writer.writeValueAsString(allGranularity) == /"all"/
    }

    def "All granularity iterates over simplified intervals without otherwise changing them"() {
        List<Interval> rawIntervals = [
                new Interval(new DateTime("2015"), Weeks.THREE),
                new Interval(new DateTime("2017"),Hours.ONE),
                new Interval(new DateTime("2013"),Weeks.TWO)
        ]

        when:
        Iterator<Interval> expectedIntervals = new SimplifiedIntervalList(rawIntervals).iterator()
        Iterator<Interval> actual = allGranularity.intervalsIterable(rawIntervals).iterator();

        then:
        while(expectedIntervals.hasNext()) {
            assert expectedIntervals.next() == actual.next()
        }
        ! actual.hasNext()
    }
}
