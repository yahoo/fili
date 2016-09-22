// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data

import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.JsonSortStrategy

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
/**
 * ResultSet object serialization test
 */
class ResultSetSerializationProxySpec extends Specification {

    private static final ObjectMapper objectMapper = new ObjectMapper()
    SerializationResources resources
    ResultSetSerializationProxy serializeResultSet

    def setup() {
        resources = new SerializationResources().init()
        serializeResultSet = new ResultSetSerializationProxy(resources.resultSet)
    }

    def "ResultSet object custom serialization produces the expected json output"() {
        expect:
        GroovyTestUtils.compareJson(
                resources.serializedResultSet,
                objectMapper.writeValueAsString(serializeResultSet),
                JsonSortStrategy.SORT_BOTH)
    }

    def "Schema object custom serialization produces the expected json output"(){
        setup:
        Set dimensionColumns = ["ageBracket","gender","country"] as Set
        Set metricColumns = ["simplePageViews","lookbackPageViews", "retentionPageViews"] as Set
        Map schema = ["granularity":"day", "timeZone":"UTC"]
        schema.put("dimensionColumns",dimensionColumns)
        schema.put("metricColumns", metricColumns)

        expect:
        GroovyTestUtils.compareObjects(serializeResultSet.getSchemaComponents(resources.schema),  schema)
    }
}
