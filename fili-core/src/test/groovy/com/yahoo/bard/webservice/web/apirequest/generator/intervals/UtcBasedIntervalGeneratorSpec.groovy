// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.intervals

import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.util.DateTimeFormatterFactory
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException
import com.yahoo.bard.webservice.web.apirequest.generator.UnsatisfiedApiRequestConstraintsException

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.Period
import org.joda.time.format.DateTimeFormatter

import spock.lang.Specification
import spock.lang.Unroll

class UtcBasedIntervalGeneratorSpec extends Specification {

    UtcBasedIntervalGenerator generator = new UtcBasedIntervalGenerator()
    DateTimeFormatter formatter = DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER

    static DateTime now
    static DateTimeZone original
    static Period oneDay = new Period("P1D")

    def setupSpec() {
        original = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.UTC)
        now = new DateTime(0)
    }

    def cleanupSpec() {
        DateTimeZone.setDefault(original)
    }

    def "Validate fails if the interval isn't set."() {
        setup:
        DataApiRequestBuilder builder = Mock(DataApiRequestBuilder)
        Granularity g = DefaultTimeGrain.DAY

        when:
        generator.validateGranularityPresent(builder)

        then:
        1 * builder.isGranularityInitialized() >> false
        thrown(UnsatisfiedApiRequestConstraintsException)

        when:
        generator.validateGranularityPresent(builder)

        then:
        1 * builder.isGranularityInitialized() >> true
        1 * builder.getGranularityIfInitialized() >> Optional.empty()
        Exception e = thrown(BadApiRequestException)
        e.getMessage().equals(UtcBasedIntervalGenerator.GRANULARITY_REQUIRED)

        when:
        Granularity actual = generator.validateGranularityPresent(builder)

        then:
        1 * builder.isGranularityInitialized() >> true
        builder.getGranularityIfInitialized() >> Optional.of(g)
        actual == g
    }


    def "Validate fails if the interval doesn't align"() {
        setup:
        Granularity g = DefaultTimeGrain.DAY
        DateTime dateTime = new DateTime(-1)
        Interval i = new Interval(dateTime, g.getPeriod())

        when:
        UtcBasedIntervalGenerator.validateTimeAlignment(g, Collections.singletonList(i))

        then:
        Exception e = thrown(BadApiRequestException)
        e.getMessage().contains(i.toString())
    }

    @Unroll
    def "GenerateIntervals for #request produces #intervals"() {
        setup:
        Granularity g = DefaultTimeGrain.DAY
        List<Interval> result = UtcBasedIntervalGenerator.generateIntervals(now, request, g, formatter)

        expect:
        intervals == result

        where:
        request                    | intervals
        "P1D/Current"              | [new Interval(oneDay, now)]
        "P1D/Current,Current/Next" | [new Interval(now.minusDays(1), now.plusDays(1))]
        "Current/Next"             | [new Interval(now, oneDay)]
        "1970/P1D,1971/P1D"        | [new Interval(now, oneDay), new Interval(now.plusYears(1), oneDay)]
        "1970/1980"                | [new Interval(now, now.plusYears(10))]
        "1970/RRULE=FREQ=MONTHLY;BYMONTHDAY=-1;COUNT=3"         |
                [new Interval(now.plusMonths(1).minusDays(1), oneDay),
                 new Interval(now.plusMonths(2).minusDays(1), oneDay),
                 new Interval(now.plusMonths(3).minusDays(1), oneDay),
                ]
        "Current/RRULE=FREQ=MONTHLY;BYMONTHDAY=-1;COUNT=3"         |
                [new Interval(now.plusMonths(1).minusDays(1), oneDay),
                 new Interval(now.plusMonths(2).minusDays(1), oneDay),
                 new Interval(now.plusMonths(3).minusDays(1), oneDay),
                ]
        "Current/RRULE=FREQ=MONTHLY;BYMONTHDAY=-1;COUNT=3"         |
                [new Interval(now.plusMonths(1).minusDays(1), oneDay),
                 new Interval(now.plusMonths(2).minusDays(1), oneDay),
                 new Interval(now.plusMonths(3).minusDays(1), oneDay),
                ]
    }

    def "Rrule doesn't overrun size"() {
        setup:
        String original = SystemConfigProvider.getInstance().setProperty("bard__rrule_max_occurences", "17")
        Granularity g = DefaultTimeGrain.DAY
        String interval = "Current/RRULE=FREQ=MONTHLY;BYMONTHDAY=-1"
        List<Interval> result = UtcBasedIntervalGenerator.generateIntervals(now, interval, g, formatter)

        expect:
        result.size() == 17

        cleanup:
        SystemConfigProvider.getInstance().resetProperty("bard__rrule_max_occurences", original)
    }
}
