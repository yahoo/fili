// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.serializers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.utils.JsonTestUtils
import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.DruidQueryBuilder
import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

import org.joda.time.DateTime
import org.joda.time.Hours
import org.joda.time.Interval

import spock.lang.Specification

class FlagFromTagDimensionToDimensionSpecSpec extends Specification {

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
        builder = new DruidQueryBuilder(
                resources.logicalDictionary,
                resolver,
                resources.druidFilterBuilder,
                resources.druidHavingBuilder
        )
        apiRequest = Mock(DataApiRequest)
        LogicalMetric lm1 = new LogicalMetricImpl(resources.simpleTemplateQuery, new NoOpResultSetMapper(), "lm1", null)

        apiRequest.getTable() >> resources.lt14
        apiRequest.getGranularity() >> HOUR.buildZonedTimeGrain(UTC)
        apiRequest.getTimeZone() >> UTC
        apiRequest.getLogicalMetrics() >> ([lm1])
        apiRequest.getIntervals() >> [new Interval(new DateTime("2015"), Hours.ONE)]
        apiRequest.getFilterDimensions() >> []
        apiRequest.getTopN() >> Optional.empty()
        apiRequest.getSorts() >> ([])
        apiRequest.getCount() >> Optional.empty()
        apiRequest.getApiFilters() >> []

        apiRequest.withFilters(_) >> {apiRequest}
    }

    def "Flag from tag dimension based on lookup dimension serializes to cascade extraction function with tag extraction and transformation in grouping context"() {
        given:
        apiRequest.getDimensions() >> ([resources.d14])
        apiRequest.getAllGroupingDimensions() >> ([resources.d14])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        when:
        ArrayNode dimensionsSerialization = (ArrayNode) objectMapper.valueToTree(druidQuery).findParent("dimensions").get("dimensions")

        then:
        JsonTestUtils.contains(dimensionsSerialization, lookupSerialization)
    }

    def "Flag from tag dimension based on *registered* lookup dimension serializes to cascade extraction function with tag extraction and transformation in grouping context"() {
        given:
        apiRequest.getDimensions() >> ([resources.d15])
        apiRequest.getAllGroupingDimensions() >> ([resources.d15])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        when:
        ArrayNode dimensionsSerialization = (ArrayNode) objectMapper.valueToTree(druidQuery).findParent("dimensions").get("dimensions")

        then:
        JsonTestUtils.contains(dimensionsSerialization, registeredLookupSerialization)
    }

    def "Flag from tag dimension pushes extraction and transformation to innermost query when nesting"() {
        setup:
        LogicalMetric nestedLogicalMetric = new LogicalMetricImpl(resources.simpleNestedTemplateQuery, new NoOpResultSetMapper(), "lm1", null)
        JsonNode expectedInnerFlagFromTagSerialization = registeredLookupSerialization
        JsonNode expectedOuterFlagFromTagSerialization = objectMapper.readValue('"flagFromTagRegisteredLookup"', JsonNode.class)

        when:
        // generate queries
        apiRequest.getDimensions() >> ([resources.d15])
        apiRequest.getAllGroupingDimensions() >> ([resources.d15])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleNestedTemplateQuery)
        DruidAggregationQuery<?> innerQuery = druidQuery.getInnerQuery().orElseThrow({return new IllegalStateException("Query is not nested")})
        DruidAggregationQuery<?> outerQuery = druidQuery

        // convert to json nodes
        JsonNode innerQueryJson = objectMapper.valueToTree(innerQuery)
        JsonNode outerQueryJson = objectMapper.valueToTree(outerQuery)
        // remove the inner query from the outer query. We are not checking datasource serialization so this doesn't affect any testing
        ((ObjectNode) outerQueryJson).remove("dataSource")

        // get the dimensions serialization for each node
        ArrayNode innerQueryDimensionsSerialization = (ArrayNode) innerQueryJson.findParent("dimensions").get("dimensions")
        ArrayNode outerQueryDimensionSerialization = (ArrayNode) outerQueryJson.findParent("dimensions").get("dimensions")

        then: "rebind logical metrics to use metric with nested tdq"
        apiRequest.getLogicalMetrics() >> ([nestedLogicalMetric])

        and: "inner query contains only extraction functions on the regex column"
        JsonTestUtils.contains(innerQueryDimensionsSerialization, expectedInnerFlagFromTagSerialization)
        ! JsonTestUtils.contains(innerQueryDimensionsSerialization, expectedOuterFlagFromTagSerialization)

        and: "outer query contains just the dimension name of the flag from tag dimension"
        JsonTestUtils.contains(outerQueryDimensionSerialization, expectedOuterFlagFromTagSerialization)
        ! JsonTestUtils.contains(outerQueryDimensionSerialization, expectedInnerFlagFromTagSerialization)
    }

    def getLookupSerialization() {
        String expectedSerialization = '''
        {
          "type": "extraction",
          "dimension": "shape",
          "outputName": "flagFromTagLookup",
          "extractionFn": {
            "type": "cascade",
            "extractionFns": [
              {
                "type": "lookup",
                "lookup": {
                  "type": "namespace",
                  "namespace": "SPECIES"
                },
                "retainMissingValue": false,
                "replaceMissingValueWith": "Unknown SPECIES",
                "injective": false,
                "optimize": true
              },
              {
                "type": "regex",
                "expr": "^(.+,)*(TAG_VALUE)(,.+)*$",
                "index": 2,
                "replaceMissingValue": true,
                "replaceMissingValueWith": ""
              },
              {
                "type": "lookup",
                "lookup": {
                  "type": "map",
                  "map": {
                    "TAG_VALUE": "TRUE_VALUE"
                  }
                },
                "retainMissingValue": false,
                "replaceMissingValueWith": "FALSE_VALUE",
                "injective": false,
                "optimize": false
              }
            ]
          }
        }'''
        objectMapper.readValue(expectedSerialization, JsonNode.class)
    }

    def getRegisteredLookupSerialization() {
        String expectedSerialization = '''
        {
          "type": "extraction",
          "dimension": "breed",
          "outputName": "flagFromTagRegisteredLookup",
          "extractionFn": {
            "type": "cascade",
            "extractionFns": [
              {
                "type": "registeredLookup",
                "lookup": "BREED__SPECIES",
                "retainMissingValue": false,
                "replaceMissingValueWith": "Unknown BREED__SPECIES",
                "injective": false,
                "optimize": true
              },
              {
                "type": "registeredLookup",
                "lookup": "BREED__OTHER",
                "retainMissingValue": false,
                "replaceMissingValueWith": "Unknown BREED__OTHER",
                "injective": false,
                "optimize": true
              },
              {
                "type": "registeredLookup",
                "lookup": "BREED__COLOR",
                "retainMissingValue": false,
                "replaceMissingValueWith": "Unknown BREED__COLOR",
                "injective": false,
                "optimize": true
              },
              {
                "type": "regex",
                "expr": "^(.+,)*(TAG_VALUE)(,.+)*$",
                "index": 2,
                "replaceMissingValue": true,
                "replaceMissingValueWith": ""
              },
              {
                "type": "lookup",
                "lookup": {
                  "type": "map",
                  "map": {
                    "TAG_VALUE": "TRUE_VALUE"
                  }
                },
                "retainMissingValue": false,
                "replaceMissingValueWith": "FALSE_VALUE",
                "injective": false,
                "optimize": false
              }
            ]
          }
        }'''
        objectMapper.readValue(expectedSerialization, JsonNode.class)
    }
}
