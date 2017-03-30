// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.GROUP_BY
import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.TIMESERIES
import static com.yahoo.bard.webservice.druid.model.DefaultQueryType.TOP_N
import static org.hamcrest.Matchers.both
import static org.hamcrest.Matchers.greaterThan
import static org.hamcrest.Matchers.is
import static org.hamcrest.number.OrderingComparison.lessThan
import static spock.util.matcher.HamcrestSupport.that

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigProvider
import com.yahoo.bard.webservice.data.DruidQueryBuilder
import com.yahoo.bard.webservice.data.PartialDataHandler
import com.yahoo.bard.webservice.data.config.ConfigurationLoader
import com.yahoo.bard.webservice.data.dimension.BardDimensionField
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary
import com.yahoo.bard.webservice.data.filterbuilders.DefaultDruidFilterBuilder
import com.yahoo.bard.webservice.data.metric.TemplateDruidQueryMerger
import com.yahoo.bard.webservice.data.time.StandardGranularityParser
import com.yahoo.bard.webservice.data.volatility.VolatileIntervalsService
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.aggregation.SketchCountAggregation
import com.yahoo.bard.webservice.table.resolver.DefaultPhysicalTableResolver
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.PathSegment

class WeightEvaluationQuerySpec extends Specification {

    JerseyTestBinder jtb
    TemplateDruidQueryMerger merger = new TemplateDruidQueryMerger()
    DruidQueryBuilder builder
    DataServlet dataServlet

    PathSegment size = Mock(PathSegment)
    PathSegment shape = Mock(PathSegment)
    PathSegment color = Mock(PathSegment)
    PathSegment other = Mock(PathSegment)

    SystemConfig systemConfig = SystemConfigProvider.getInstance()

    // other is configured with NoOpSearchProvider and its cardinality is package_name__query_weight_limit
    int queryWeightLimit = systemConfig.getIntProperty(
            systemConfig.getPackageVariableName("query_weight_limit"),
            100000
    );

    def setup() {
        jtb = new JerseyTestBinder(DataServlet.class)
        // create 2 dimensionRows per dimension = 8 total
        DimensionDictionary dimensionStore = jtb.configurationLoader.dimensionDictionary
        ["size", "shape", "color"].each { String dimensionId ->
            dimensionStore.findByApiName(dimensionId).with {
                addDimensionRow(BardDimensionField.makeDimensionRow(it, dimensionId + "1", dimensionId + "1_desc"))
                addDimensionRow(BardDimensionField.makeDimensionRow(it, dimensionId + "2", dimensionId + "2_desc"))
            }
        }

        dataServlet = Mock(DataServlet)
        ConfigurationLoader configurationLoader = jtb.configurationLoader
        dataServlet.getMetricDictionary() >> configurationLoader.metricDictionary
        dataServlet.getDimensionDictionary() >> configurationLoader.dimensionDictionary
        dataServlet.getLogicalTableDictionary() >> configurationLoader.logicalTableDictionary
        dataServlet.getFilterBuilder() >> new DefaultDruidFilterBuilder()
        dataServlet.getGranularityParser() >> new StandardGranularityParser()

        builder = new DruidQueryBuilder(
                jtb.configurationLoader.logicalTableDictionary,
                new DefaultPhysicalTableResolver((PartialDataHandler) null, (VolatileIntervalsService) null)
        )

        Map emptyMap = new MultivaluedHashMap<>()
        size.getPath() >> "size"
        size.getMatrixParameters() >> emptyMap
        shape.getPath() >> "shape"
        shape.getMatrixParameters() >> emptyMap
        color.getPath() >> "color"
        color.getMatrixParameters() >> emptyMap
        other.getPath() >> "other"
        other.getMatrixParameters() >> emptyMap
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def "Worst case estimate calculation for day"() {
        given: "outer daily use daily time grain"
        final DataApiRequest apiRequest = new DataApiRequest(
                "shapes",
                "day",
                [size, shape, color, other],
                "users",
                "2014-09-01/2014-09-30",
                null, //filters
                null, //havings
                null, //sorts
                null, //counts
                null, //topN
                null, //format
                null, //timeZoneId
                null, //asyncAfter
                "", //perPage
                "", //page
                null, //uriInfo
                dataServlet
        )

        GroupByQuery groupByQuery = builder.buildQuery(apiRequest, merger.merge(apiRequest))

        expect: "Worst case is # of Sketches * # of days * Cardinality of all dimensions"
        WeightEvaluationQuery.getWorstCaseWeightEstimate(groupByQuery) == 1 * 29 * 8 * queryWeightLimit
    }


    def "Worst case estimate calculation for daily average aggregated to week"() {
        given: "weekly day average in outer query, use inner daily time grain"
        final DataApiRequest apiRequest = new DataApiRequest(
                "shapes",
                "week",
                [size, shape, color, other],
                "dayAvgUsers",
                "2014-09-01/2014-09-29",
                null, //filters
                null, //havings
                null, //sorts
                null, //counts
                null, //topN
                null, //format
                null, //timeZoneId
                null, //asyncAfter
                "", //perPage
                "", //page
                null, //uriInfo
                dataServlet
        )

        GroupByQuery groupByQuery = builder.buildQuery(apiRequest, merger.merge(apiRequest))

        expect: "Worst case is # of Sketches * # of days * Cardinality of all daily dimensions"
        WeightEvaluationQuery.getWorstCaseWeightEstimate(groupByQuery) == 1 * 28 * 8 * queryWeightLimit
    }

    def "Worst case estimate calculation for weekly aggregate"() {
        given: "weekly day average in outer query, use inner daily timegrain"
        final DataApiRequest apiRequest = new DataApiRequest(
                "shapes",
                "week",
                [size, shape, color, other],
                "users,otherUsers",
                "2014-09-01/2014-09-29",
                null, //filters
                null, //havings
                null, //sorts
                null, //counts
                null, //topN
                null, //format
                null, //timeZoneId
                null, //asyncAfter
                "", //perPage
                "", //page
                null, //uriInfo
                dataServlet
        )

        GroupByQuery groupByQuery = builder.buildQuery(apiRequest, merger.merge(apiRequest))

        expect: "Worst case is # of Sketches * # of weeks * Cardinality of all dimensions"
        WeightEvaluationQuery.getWorstCaseWeightEstimate(groupByQuery) == 2 * 4 * 8 * queryWeightLimit
    }

    def "Weight check query strips sort columns"() {
        given: "weekly day average in outer query, use inner daily timegrain"
        final DataApiRequest apiRequest = new DataApiRequest(
                "shapes",
                "week",
                [size, shape, color, other],
                "users,otherUsers",
                "2014-09-01/2014-09-29",
                null, //filters
                null, //havings
                "users", //sorts
                null, //counts
                null, //topN
                null, //format
                null, //timeZoneId
                null, //asyncAfter
                "", //perPage
                "", //page
                null, //uriInfo
                dataServlet
        )

        GroupByQuery groupByQuery = builder.buildQuery(apiRequest, merger.merge(apiRequest))

        expect: "The groupBy query has a sort, as a pre-condition"
        groupByQuery.limitSpec.columns

        and: "The weight-check query doesn't have a sort at either level"
        !new WeightEvaluationQuery(groupByQuery, 1).limitSpec.columns
        !new WeightEvaluationQuery(groupByQuery, 1).innermostQuery.limitSpec.columns
    }

    @Unroll
    def "Worst estimate for big dimensions works if estimate is larger than an int but not a long for #queryType"() {
        given: "A query with one sketch, a small number of time buckets, and dimensions with very large cardinality"
        DruidAggregationQuery query = getLargeDimensionQueryStub(YEAR, queryType)

        expect: "The estimate is larger than an int but not too large for a long"
        that WeightEvaluationQuery.getWorstCaseWeightEstimate(query), is(
                both(lessThan(Long.MAX_VALUE)).and(greaterThan(Integer.MAX_VALUE as Long))
        )

        where:
        queryType << [GROUP_BY, TOP_N, TIMESERIES]
    }

    @Unroll
    def "Worst estimate for big dimensions throws an exception if estimate is larger than a long for #queryType"() {
        given: "A query with one sketch, a large number of time buckets, and dimensions with very large cardinality"
        DruidAggregationQuery query = getLargeDimensionQueryStub(DAY, queryType)

        when: "We get the worst case weight estimate"
        WeightEvaluationQuery.getWorstCaseWeightEstimate(query)

        then:
        thrown ArithmeticException

        where:
        queryType | _
        GROUP_BY  | _
        // TOP_N  | _ // Since TopN only has 1 dimension, it's _much_ harder to hit this condition
        // TIME_SERIES  | _ // Same for timeseries
    }

    DruidAggregationQuery getLargeDimensionQueryStub(Granularity granularity, DefaultQueryType queryType) {
        // Dimension stubs with large cardinalities
        Dimension d1 = Stub(Dimension)
        Dimension d2 = Stub(Dimension)
        d1.getCardinality() >> Integer.MAX_VALUE
        d2.getCardinality() >> Integer.MAX_VALUE

        // TopNQuery stub
        DruidAggregationQuery query = Stub(TopNQuery)
        with(query) {
            getDimensions() >> [d1, d2]
            getDimension() >> d1
            getThreshold() >> Long.MAX_VALUE
            getAggregations() >> [Stub(SketchCountAggregation)]
            getIntervals() >> [new Interval("2014/2016")]
            getGranularity() >> granularity
            getQueryType() >> queryType
            getInnermostQuery() >> query
        }

        return query
    }
}
