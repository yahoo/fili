// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.volatility

import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.LongSupplier
/**
 * Test to ensure that time offset returns volatile intervals
 */
class TimeOffsetVolatileIntervalsFunctionSpec extends Specification {

    static final long NOW = System.currentTimeMillis()
    static final long PAST = 10 * 60 * 1000;
    static final long FUTURE = 10 * 1000;

    @Shared TimeOffsetVolatileIntervalsFunction offsetService;
    @Shared TimeOffsetVolatileIntervalsFunction paddedService;

    static LongSupplier originalTimeSupplier = TimeOffsetVolatileIntervalsFunction.currentTimeMillis

    static SimplifiedIntervalList buildIntervalList(long from, long to) {
        return new SimplifiedIntervalList(Collections.singleton(new Interval(from, to)))
    }

    def setupSpec() {
        TimeOffsetVolatileIntervalsFunction.currentTimeMillis = {-> NOW}
        offsetService = new TimeOffsetVolatileIntervalsFunction(PAST)
        paddedService = new TimeOffsetVolatileIntervalsFunction(PAST, FUTURE)
    }

    def cleanupSpec() {
        TimeOffsetVolatileIntervalsFunction.currentTimeMillis = originalTimeSupplier;
    }

    @Unroll
    def "With #service getVolatileIntervals returns #intervalsList"() {
        expect:
        service.getVolatileIntervals() == intervalsList

        where:
        service        | intervalsList
        offsetService  | buildIntervalList(NOW-PAST, NOW + TimeOffsetVolatileIntervalsFunction.FAR_FUTURE)
        paddedService  | buildIntervalList(NOW-PAST, NOW+FUTURE)
    }
}
