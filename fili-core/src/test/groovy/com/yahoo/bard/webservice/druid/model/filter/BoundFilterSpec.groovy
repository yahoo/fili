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

    def "Checking serialization of null values"() {
        given:
        BoundFilter filter = new BoundFilter(dimension, null, null, null, null, null);
        druidQuery.getFilter() >> filter
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerialization)
    }

    def "Checking the functioning of Bound Filter"() {
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

    def "Checking the functioning of nestWith()"() {
        given:
        BoundFilter filter = new BoundFilter(
                dimension,
                null,
                null,
                null,
                null,
                null
        )
        BoundFilter fil2 = filter.withLowerBound("10.0").withUpperBound("13.0").withUpperBoundStrict(false).withLowerBoundStrict(true).withOrdering(Ordering.NUMERIC)
        druidQuery.getFilter() >> fil2
        String serializedFilter = objectMapper.writeValueAsString(druidQuery)

        expect:
        objectMapper.readTree(serializedFilter).get("filter") == objectMapper.readTree(expectedSerializationBoundFilter)
    }
}
