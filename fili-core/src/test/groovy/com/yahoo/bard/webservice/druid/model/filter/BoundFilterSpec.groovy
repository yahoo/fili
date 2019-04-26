// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter

import com.yahoo.bard.webservice.data.dimension.impl.LookupDimension
import com.yahoo.bard.webservice.druid.model.Ordering
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
class BoundFilterSpec extends Specification {

    ObjectMapper objectMapper
    LookupDimension dimension
    DruidAggregationQuery druidQuery

    ExtractionFunction extractionFunction

    // Dimension missing due to lack of proper way to inject mock dimension value and therefore null is given
    String expectedSerialization =
            """
                {
                    "dimension": "foo",
                    "type":"bound",
                    "extractionFn":{
                        "type":"registeredLookup",
                        "lookup":"lookup",
                        "retainMissingValue":false,
                        "replaceMissingValueWith":"none",
                        "injective":false,
                        "optimize":false
                    }
                }
            """

    // Serialization of an actual Bound Filter
    String expectedSerializationBoundFilter =
            """
                {
                    "dimension": "foo",
                    "type":"bound",
                    "lower":"10.0",
                    "upper":"13.0",
                    "lowerStrict":true,
                    "upperStrict":false,
                    "ordering":"numeric",
                    "extractionFn":{
                        "type":"registeredLookup",
                        "lookup":"lookup",
                        "retainMissingValue":false,
                        "replaceMissingValueWith":"none",
                        "injective":false,
                        "optimize":false
                    }
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

    def "Null values are omitted when serializing"() {
        given:
        BoundFilter filter = new BoundFilter(dimension, null, null, null, null, null);
        druidQuery.getFilter() >> filter
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerialization)
    }

    def "Sample values to the Bound Filter serialize correctly"() {
        given:
        BoundFilter filter = new BoundFilter(
                dimension,
                "10.0",
                "13.0",
                true,
                false,
                Ordering.NUMERIC
        )
        druidQuery.getFilter() >> filter
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerializationBoundFilter)
    }

    def "With methods makes updating copies"() {
        given:
        BoundFilter filter = new BoundFilter(
                dimension,
                null,
                null,
                null,
                null,
                null
        )
        BoundFilter withFilters =
                filter
                    .withLowerBound("10.0")
                    .withUpperBound("13.0")
                    .withUpperBoundStrict(false)
                    .withLowerBoundStrict(true)
                    .withOrdering(Ordering.NUMERIC)
        druidQuery.getFilter() >> withFilters
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerializationBoundFilter)
    }

    def "Static buildUpperBoundFilter() works as expected"() {
        given:
        BoundFilter upperBoundFilter = BoundFilter.buildUpperBoundFilter(dimension, "20.0", true)
        String expectedSerializationUpperBoundFilter =
                """
                    {
                        "dimension": "foo",
                        "type":"bound",
                        "upper":"20.0",
                        "upperStrict":false,
                        "extractionFn":{
                            "type":"registeredLookup",
                            "lookup":"lookup",
                            "retainMissingValue":false,
                            "replaceMissingValueWith":"none",
                            "injective":false,
                            "optimize":false
                        }
                    }
                """
        druidQuery.getFilter() >> upperBoundFilter
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerializationUpperBoundFilter)
    }

    def "Static buildLowerBoundFilter() works as expected"() {
        given:
        BoundFilter lowerBoundFilter = BoundFilter.buildLowerBoundFilter(dimension, "20.0", false)
        String expectedSerializationLowerBoundFilter =
                """
                    {
                        "dimension": "foo",
                        "type":"bound",
                        "lower":"20.0",
                        "lowerStrict":true,
                        "extractionFn":{
                            "type":"registeredLookup",
                            "lookup":"lookup",
                            "retainMissingValue":false,
                            "replaceMissingValueWith":"none",
                            "injective":false,
                            "optimize":false
                        }
                    }
                """
        druidQuery.getFilter() >> lowerBoundFilter
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerializationLowerBoundFilter)
    }
}
