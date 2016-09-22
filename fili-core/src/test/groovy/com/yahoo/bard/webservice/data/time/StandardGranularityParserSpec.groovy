// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.util.GranularityParseException
import com.yahoo.bard.webservice.web.ErrorMessageFormat

import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Tests for the StandardGranularityParser class.
 */
class StandardGranularityParserSpec extends Specification {

    StandardGranularityParser granularityParser = new StandardGranularityParser()
    static final String DENVER_NAME = "America/Denver"
    static final DateTimeZone DENVER_TZ = DateTimeZone.forID(DENVER_NAME)

    @Unroll
    def "check standard time context parses (#name) into granularity #expected "() {
        expect:
        granularityParser.parseGranularity(name) == expected

        where:
        name    | expected
        "day"   | DAY
        "month" | MONTH
        "all"   | AllGranularity.INSTANCE
    }

    @Unroll
    def "check standard time context parses (#name, UTC) into zoned granularity #expected "() {
        expect:
        granularityParser.parseGranularity(name, UTC) == expected

        where:
        name    | expected
        "day"   | new ZonedTimeGrain(DAY, UTC)
        "month" | new ZonedTimeGrain(MONTH, UTC)
        "all"   | AllGranularity.INSTANCE
    }

    def "check invalid granularity creates error"() {
        setup: "Define an improper granularity name"
        String timeGrainName = "seldom"
        String expectedMessage = ErrorMessageFormat.UNKNOWN_GRANULARITY.format(timeGrainName)

        when:
        granularityParser.parseGranularity(timeGrainName)

        then:
        Exception e = thrown(GranularityParseException)
        e.getMessage() == expectedMessage
    }
}
