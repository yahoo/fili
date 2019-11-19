// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTimeZone

import spock.lang.Specification

/**
 * Tests for TimeFormatExtractionFunction serializations.
 */
class TimeFormatExtractionFunctionSpec extends Specification {

    ObjectMapper objectMapper

    def setup() {
        objectMapper = new ObjectMappersSuite().getMapper()
    }

    def "TimeFormatExtractionFunction with no optional fields serializes to default extraction function"() {
        expect:
        GroovyTestUtils.compareJson(
                objectMapper.writeValueAsString(new TimeFormatExtractionFunction()),
                """{"type":"${ExtractionFunction.DefaultExtractionFunctionType.TIME_FORMAT.jsonName}"}"""
        )
    }

    def "TimeFormatExtractionFunction with all optional fields set serializes to a complete extraction function"() {
        expect:
        GroovyTestUtils.compareJson(
                objectMapper.writeValueAsString(
                        new TimeFormatExtractionFunction(
                                "YYYYMM",
                                Locale.ENGLISH,
                                DateTimeZone.UTC,
                                DefaultTimeGrain.DAY,
                                Boolean.TRUE
                        )
                ),
                """
                    {
                        "type":"${ExtractionFunction.DefaultExtractionFunctionType.TIME_FORMAT.jsonName}",
                        "format":"YYYYMM",
                        "locale":"${Locale.ENGLISH}",
                        "timeZone":"${DateTimeZone.UTC}",
                        "granularity":{"type":"period","period":"P1D"},
                        "asMillis":${Boolean.TRUE}
                    }
                """
        )
    }
}
