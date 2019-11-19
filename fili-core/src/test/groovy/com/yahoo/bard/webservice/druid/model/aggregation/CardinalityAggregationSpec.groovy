// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.aggregation

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.TableTestUtils
import com.yahoo.bard.webservice.util.GroovyTestUtils

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.commons.lang3.tuple.Pair
import org.joda.time.DateTimeZone

import spock.lang.Specification

class CardinalityAggregationSpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMappersSuite().getMapper()

    static String expectedJson = """
    { "type":"cardinality", "name":"name", "fieldNames": ["d1DruidName", "d2DruidName"], "byRow": true}
    """
    Dimension d1
    Dimension d2
    CardinalityAggregation a1

    ConstrainedTable constrainedTable

    def setupSpec() {
        MAPPER.readTree(expectedJson)
    }

    def setup() {
        constrainedTable = TableTestUtils.buildTable(
                "table",
                DefaultTimeGrain.DAY.buildZonedTimeGrain(DateTimeZone.UTC),
                [] as Set,
                ["d1ApiName": "d1DruidName", "d2ApiName": "d2DruidName"],
                Mock(DataSourceMetadataService) { getAvailableIntervalsByDataSource(_ as DataSourceName) >> [:]}
        )
        d1 = Mock(Dimension)
        d2 = Mock(Dimension)

        d1.getApiName() >> "d1ApiName"
        d2.getApiName() >> "d2ApiName"
        a1 = new CardinalityAggregation("name", [d1, d2] as LinkedHashSet, true)
    }

    def "verify cardinality aggregation nests correctly"() {
        setup:
        Aggregation a1 = new CardinalityAggregation("name", new LinkedHashSet<Dimension>(), true)

        when:
        Pair<Optional<Aggregation>, Optional<Aggregation>> nested = a1.nest()

        then:
        nested.getLeft().get() == a1
        nested.getRight() == Optional.empty()
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
        setup:
        // NOTE: Dimensions cannot be serialized without a query (for physical table mapping).
        //       Consequently, query's also need this to be serialized.
        DruidAggregationQuery query = Mock(DruidAggregationQuery)
        DataSource ds = Mock(DataSource)
        ds.getQuery() >> Optional.empty()
        ds.getPhysicalTable() >> constrainedTable
        query.dataSource >> ds
        query.aggregations >> [a1]

        String value = MAPPER.writeValueAsString(MAPPER.readTree(MAPPER.writer().writeValueAsString(query)).get("aggregations").get(0));

        expect:
        GroovyTestUtils.compareJson(value, expectedJson)
    }
}
