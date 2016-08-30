// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.data.DruidQueryBuilder
import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.dimension.Dimension
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
 * Testing dimension serialization to default dimension spec.
 */
class DimensionToDefaultDimensionSpecSpec extends Specification {

    ObjectMapper objectMapper
    QueryBuildingTestingResources resources
    DruidQueryBuilder builder
    DataApiRequest apiRequest
    DruidAggregationQuery<?> druidQuery

    def setup() {
        objectMapper = new ObjectMapper()
        resources = new QueryBuildingTestingResources()
        DefaultPhysicalTableResolver resolver = new DefaultPhysicalTableResolver(new PartialDataHandler(), new DefaultingVolatileIntervalsService())
        builder = new DruidQueryBuilder(resources.logicalDictionary, resolver)
        apiRequest = Mock(DataApiRequest)
        LogicalMetric lm1 = new LogicalMetric(resources.simpleTemplateQuery, new NoOpResultSetMapper(), "lm1", null)

        apiRequest.getTable() >> resources.lt12
        apiRequest.getGranularity() >> HOUR.buildZonedTimeGrain(UTC)
        apiRequest.getTimeZone() >> UTC
        apiRequest.getLogicalMetrics() >> ([lm1])
        apiRequest.getIntervals() >> [new Interval(new DateTime("2015"), Hours.ONE)]
        apiRequest.getFilterDimensions() >> []
        apiRequest.getTopN() >> OptionalInt.empty()
        apiRequest.getSorts() >> ([])
        apiRequest.getCount() >> OptionalInt.empty()
    }

    def "Serialize to apiName when apiName and physicalName of a dimension is the same"() {
        given:
        apiRequest.getDimensions() >> ([resources.d1])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        expect:
        objectMapper.writeValueAsString(druidQuery).contains('"dimensions":["dim1"]')
    }

    def "Serialize to dimension spec when the apiName and physicalName of a dimension is different"() {
        given:
        apiRequest.getDimensions() >> ([resources.d3])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        expect:
        objectMapper.writeValueAsString(druidQuery).contains('"dimensions":[{"type":"default","dimension":"age_bracket","outputName":"ageBracket"}]')
    }

    def "Serialize dimension of a nested query should only serialize the innermost query's dimension to a dimension spec"() {
        given:
        apiRequest.getDimensions() >> ([resources.d3])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleNestedTemplateQuery)
        String serializedQuery = objectMapper.writeValueAsString(druidQuery)

        expect:
        serializedQuery.contains('"dimensions":[{"type":"default","dimension":"age_bracket","outputName":"ageBracket"}]') && serializedQuery.contains('"dimensions":["ageBracket"]')
    }
}
