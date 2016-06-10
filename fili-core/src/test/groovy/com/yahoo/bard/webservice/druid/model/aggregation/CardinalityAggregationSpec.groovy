// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import spock.lang.Specification

class CardinalityAggregationSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    static String expectedJson = """
    { "type":"cardinality", "name":"name", "fieldNames": ["d1DruidName", "d2DruidName"], "byRow": true}
    """
    Dimension d1
    Dimension d2
    CardinalityAggregation a1

    def setupSpec() {
        MAPPER.readTree(expectedJson)
    }

    def setup() {
        d1 = Mock(Dimension)
        d2 = Mock(Dimension)

        d1.getApiName() >> "d1ApiName"
        d1.getDruidName() >> "d1DruidName"
        d2.getApiName() >> "d2ApiName"
        d2.getDruidName() >> "d2DruidName"
        a1 = new CardinalityAggregation("name", [d1, d2] as Set, true)
    }

    def "verify nest throws exception"() {
        when:
        a1.nest()

        then:
        thrown(UnsupportedOperationException)
    }

    def "Test with field throws exception"() {
        when:
        a1.withFieldName("test")

        then:
        thrown(UnsupportedOperationException)
    }

    def "Test with methods create correct copies"() {
        expect:
        a1.withName("Test") == new CardinalityAggregation("Test",  [d1, d2] as Set, true)
        a1.withDimensions([d1] as Set) == new CardinalityAggregation("name",  [d1] as Set, true)
        a1.withByRow(false) == new CardinalityAggregation("name",  [d1, d2] as Set, false)
    }

    def "serialization is correct"() {
        expect:
        GroovyTestUtils.compareJson(expectedJson, MAPPER.writer().writeValueAsString(a1))
    }

}
