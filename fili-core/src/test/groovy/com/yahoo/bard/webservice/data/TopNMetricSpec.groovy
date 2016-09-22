// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data
import static com.yahoo.bard.webservice.druid.model.orderby.TopNMetric.TopNMetricType.ALPHA_NUMERIC
import static com.yahoo.bard.webservice.druid.model.orderby.TopNMetric.TopNMetricType.LEXICOGRAPHIC

import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification

class TopNMetricSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    def "Check json serialization"() {
        expect:
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric("pageViews")),
                    '{"type":"numeric", "metric":"pageViews"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric(LEXICOGRAPHIC, "b")),
                    '{"type":"lexicographic", "previousStop":"b"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric(ALPHA_NUMERIC, "b1")),
                    '{"type":"alphaNumeric", "previousStop":"b1"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric("pageViews", SortDirection.ASC)),
                    '{"metric":{"metric":"pageViews","type":"numeric"},"type":"inverted"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric("pageViews", SortDirection.DESC)),
                    '{"type":"numeric", "metric":"pageViews"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric(LEXICOGRAPHIC, "b", SortDirection.DESC)),
                    '{"type":"lexicographic", "previousStop":"b"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric(LEXICOGRAPHIC, "b", SortDirection.ASC)),
                    '{"metric":{"previousStop":"b","type":"lexicographic"},"type":"inverted"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric(ALPHA_NUMERIC, "b", SortDirection.DESC)),
                    '{"type":"alphaNumeric", "previousStop":"b"}'
            )
            GroovyTestUtils.compareJson(
                    MAPPER.writeValueAsString(new TopNMetric(ALPHA_NUMERIC, "b", SortDirection.ASC)),
                    '{"metric":{"previousStop":"b","type":"alphaNumeric"},"type":"inverted"}'
            )
    }
}
