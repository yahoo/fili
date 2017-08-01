// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.DruidQueryBuilder
import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver
import com.yahoo.bard.webservice.web.DataApiRequest

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTime
import org.joda.time.Hours
import org.joda.time.Interval

import spock.lang.Specification

/**
 * Testing Lookup Dimension Serialization correctness.
 */
class LookupDimensionToDimensionSpecSpec extends Specification{

    ObjectMapper objectMapper
    QueryBuildingTestingResources resources
    DruidQueryBuilder builder
    DefaultPhysicalTableResolver resolver
    DataApiRequest apiRequest
    DruidAggregationQuery<?> druidQuery

    def setup() {
        objectMapper = new ObjectMappersSuite().getMapper()
        resources = new QueryBuildingTestingResources()
        resolver = new DefaultPhysicalTableResolver(new PartialDataHandler(), new DefaultingVolatileIntervalsService())
        builder = new DruidQueryBuilder(resources.logicalDictionary, resolver)
        apiRequest = Mock(DataApiRequest)
        LogicalMetric lm1 = new LogicalMetric(resources.simpleTemplateQuery, new NoOpResultSetMapper(), "lm1", null)

        apiRequest.getTable() >> resources.lt14
        apiRequest.getGranularity() >> HOUR.buildZonedTimeGrain(UTC)
        apiRequest.getTimeZone() >> UTC
        apiRequest.getLogicalMetrics() >> ([lm1])
        apiRequest.getIntervals() >> [new Interval(new DateTime("2015"), Hours.ONE)]
        apiRequest.getFilterDimensions() >> []
        apiRequest.getTopN() >> OptionalInt.empty()
        apiRequest.getSorts() >> ([])
        apiRequest.getCount() >> OptionalInt.empty()
        apiRequest.getFilters() >> Collections.emptyMap()
    }

    def "Given lookup dimension with no namespace serialize using dimension serializer"() {
        given:
        apiRequest.getDimensions() >> ([resources.d10])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        expect:
        objectMapper.writeValueAsString(druidQuery).contains('dimensions":["color"]')
    }

    def "Given lookup dimension with one namespace serialize correctly to (lookup) extraction dimension spec"() {
        given:
        apiRequest.getDimensions() >> ([resources.d9])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        expect:
        objectMapper.writeValueAsString(druidQuery).contains('{"type":"extraction","dimension":"shape","outputName":"shape","extractionFn":{"type":"lookup","lookup":{"type":"namespace","namespace":"SPECIES"},"retainMissingValue":false,"replaceMissingValueWith":"Unknown SPECIES","injective":false,"optimize":true}}')
    }

    def "Given lookup dimension with multiple namespaces serialize correctly to (cascade) extraction dimension spec"() {
        given:
        apiRequest.getDimensions() >> ([resources.d8])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        expect:
        objectMapper.writeValueAsString(druidQuery).contains('{"type":"extraction","dimension":"size","outputName":"size","extractionFn":{"type":"cascade","extractionFns":[{"type":"lookup","lookup":{"type":"namespace","namespace":"SPECIES"},"retainMissingValue":false,"replaceMissingValueWith":"Unknown SPECIES","injective":false,"optimize":true},{"type":"lookup","lookup":{"type":"namespace","namespace":"BREED"},"retainMissingValue":false,"replaceMissingValueWith":"Unknown BREED","injective":false,"optimize":true},{"type":"lookup","lookup":{"type":"namespace","namespace":"GENDER"},"retainMissingValue":false,"replaceMissingValueWith":"Unknown GENDER","injective":false,"optimize":true}]}}')
    }

    def "Given lookup dimension with nested query, only the inner most dimension serialize to dimension spec"() {
        given:
        apiRequest.getDimensions() >> ([resources.d9])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleNestedTemplateQuery)
        String serializedQuery = objectMapper.writeValueAsString(druidQuery)

        expect: "the inner most query serialize lookup dimension to dimension spec while the outer most query serialize as dimension api name"
        serializedQuery.contains('"dimensions":[{"type":"extraction","dimension":"shape","outputName":"shape","extractionFn":{"type":"lookup","lookup":{"type":"namespace","namespace":"SPECIES"},"retainMissingValue":false,"replaceMissingValueWith":"Unknown SPECIES","injective":false,"optimize":true}}]')
        serializedQuery.contains('"dimensions":["shape"]')
    }
}

