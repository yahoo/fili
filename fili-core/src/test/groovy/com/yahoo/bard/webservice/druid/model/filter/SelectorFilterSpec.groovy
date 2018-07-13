// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter

import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.ExtractionFunction
import com.yahoo.bard.webservice.druid.model.dimension.extractionfunction.RegisteredLookupExtractionFunction
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.ConstrainedTable

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
/**
 * Test selector filter serialization.
 */
class SelectorFilterSpec extends Specification {

    ObjectMapper objectMapper
    LookupDimension dimension
    DruidAggregationQuery druidQuery

    ExtractionFunction extractionFunction

    // Dimension missing due to lack of proper way to inject mock dimension value and therefore null is given
    String expectedSerialization =
            """
                {
                    "dimension": "foo",
                    "type":"selector",
                    "extractionFn":{
                        "type":"registeredLookup",
                        "lookup":"lookup",
                        "retainMissingValue":false,
                        "replaceMissingValueWith":"none",
                        "injective":false,
                        "optimize":false
                    },
                    "value":"value"
                }
            """

    // Dimension missing due to lack of proper way to inject mock dimension value and therefore null is given
    String expectedSerialization2 =
            """
                {
                    "dimension": "foo",
                    "type":"selector",
                    "extractionFn":{
                        "type":"registeredLookup",
                        "lookup":"lookup2",
                        "retainMissingValue":false,
                        "replaceMissingValueWith":"none",
                        "injective":false,
                        "optimize":false
                    },
                    "value":"value"
                }
            """

    def setup() {
        objectMapper = new ObjectMapper()
        dimension = Mock(LookupDimension)
        extractionFunction = new RegisteredLookupExtractionFunction("lookup", false, "none", false, false)
        TableDataSource dataSource = Mock(TableDataSource)
        ConstrainedTable physicalTable = Mock(ConstrainedTable)
        dataSource.getPhysicalTable() >> physicalTable
        dataSource.getQuery() >> Optional.empty()
        physicalTable.getPhysicalColumnName(_) >> "foo"
        druidQuery = Mock(DruidAggregationQuery)
        dimension.getExtractionFunction() >> Optional.of(extractionFunction)

        druidQuery.getDataSource() >> dataSource

    }

    def "Serialization of dimension with lookups defaulted is correct"() {
        given:
        SelectorFilter filter = new SelectorFilter(dimension,"value")
        druidQuery.getFilter() >> filter
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerialization)
    }

    def "Serialization of dimension with lookups overridden is correct"() {
        given:
        SelectorFilter filter = new SelectorFilter(
                dimension,
                "value",
                new RegisteredLookupExtractionFunction("lookup2", false, "none", false, false)
        )
        druidQuery.getFilter() >> filter
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerialization2)
    }
}
