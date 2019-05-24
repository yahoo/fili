package com.yahoo.bard.webservice.druid.serializers

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static org.joda.time.DateTimeZone.UTC

import com.yahoo.bard.webservice.application.ObjectMappersSuite
import com.yahoo.bard.webservice.data.DruidQueryBuilder
import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.QueryBuildingTestingResources
import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper
import com.yahoo.bard.webservice.data.volatility.DefaultingVolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.DefaultFilterOperation
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.filters.ApiFilters

import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTime
import org.joda.time.Hours
import org.joda.time.Interval

import spock.lang.Specification

class FlagFromTagDimensionSpecSpec extends Specification {

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
        apiRequest.getApiFilters() >> new ApiFilters([(resources.d14) : [new ApiFilter(resources.d14, DefaultDimensionField.ID, DefaultFilterOperation.in, ["TRUE_VALUE"] as Collection)] as Set] as Map)
    }

    def "Flag from tag dimension based on lookup dimension serializes to cascade extraction function with tag extraction and transformation in grouping context"() {
        given:
        apiRequest.getDimensions() >> ([resources.d14])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        expect:
        objectMapper.writeValueAsString(druidQuery).contains('{"type":"extraction","dimension":"shape","outputName":"flagFromTagLookup","extractionFn":{"type":"cascade","extractionFns":[{"type":"lookup","lookup":{"type":"namespace","namespace":"SPECIES"},"retainMissingValue":false,"replaceMissingValueWith":"Unknown SPECIES","injective":false,"optimize":true},{"type":"regex","expr":"(.+,)*(TAG_VALUE)(,.+)*","index":2,"replaceMissingValue":true,"replaceMissingValueWith":""},{"type":"lookup","lookup":{"type":"map","map":{"TAG_VALUE":"TRUE_VALUE"}},"retainMissingValue":false,"replaceMissingValueWith":"FALSE_VALUE","injective":false,"optimize":false}]}}')
    }

    def "Flag from tag dimension based on *registered* lookup dimension serializes to cascade extraction function with tag extraction and transformation in grouping context"() {
        given:
        apiRequest.getDimensions() >> ([resources.d15])
        druidQuery = builder.buildQuery(apiRequest, resources.simpleTemplateQuery)

        expect:
        objectMapper.writeValueAsString(druidQuery).contains('{"type":"extraction","dimension":"breed","outputName":"flagFromTagRegisteredLookup","extractionFn":{"type":"cascade","extractionFns":[{"type":"registeredLookup","lookup":"BREED__SPECIES","retainMissingValue":false,"replaceMissingValueWith":"Unknown BREED__SPECIES","injective":false,"optimize":true},{"type":"registeredLookup","lookup":"BREED__OTHER","retainMissingValue":false,"replaceMissingValueWith":"Unknown BREED__OTHER","injective":false,"optimize":true},{"type":"registeredLookup","lookup":"BREED__COLOR","retainMissingValue":false,"replaceMissingValueWith":"Unknown BREED__COLOR","injective":false,"optimize":true},{"type":"regex","expr":"(.+,)*(TAG_VALUE)(,.+)*","index":2,"replaceMissingValue":true,"replaceMissingValueWith":""},{"type":"lookup","lookup":{"type":"map","map":{"TAG_VALUE":"TRUE_VALUE"}},"retainMissingValue":false,"replaceMissingValueWith":"FALSE_VALUE","injective":false,"optimize":false}]}}')
    }
}
