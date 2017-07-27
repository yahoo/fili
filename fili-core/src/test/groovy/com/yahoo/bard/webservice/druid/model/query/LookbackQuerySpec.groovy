// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.query

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.LongSumAggregation
import com.yahoo.bard.webservice.druid.model.datasource.QueryDataSource
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.util.GroovyTestUtils
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module

import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.Period

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class LookbackQuerySpec extends Specification {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module().configureAbsentsAsNulls(false))

    @Shared
    DateTimeZone currentTZ
    @Shared
    TimeSeriesQuery timeSeriesQuery
    @Shared
    GroupByQuery groupByQuery
    @Shared
    Aggregation pageViews
    @Shared
    List<PostAggregation> postAggregation
    @Shared
    String timeseriesExpectedString
    @Shared
    String groupByExpectedString
    @Shared
    TimeSeriesQuerySpec timeSeriesQuerySpec
    @Shared
    LookbackQuery singleIntervalSingleLookbackOffset
    @Shared
    LookbackQuery singleIntervalMultipleLookbackOffset
    @Shared
    LookbackQuery multipleIntervalSingleLookbackOffset
    @Shared
    LookbackQuery multipleIntervalMultipleLookbackOffset
    static final Interval INTERVAL_0 = Interval.parse("2015-01-01T00:00:00.000Z/2015-01-02T00:00:00.000Z")
    static final Interval INTERVAL_1 = Interval.parse("2015-01-02T00:00:00.000Z/2015-01-03T00:00:00.000Z")
    static final Interval INTERVAL_2 = Interval.parse("2015-01-03T00:00:00.000Z/2015-01-04T00:00:00.000Z")
    static final Interval INTERVAL_3 = Interval.parse("2014-12-27T00:00:00.000Z/2014-12-28T00:00:00.000Z")
    static final Interval INTERVAL_4 = Interval.parse("2014-12-26T00:00:00.000Z/2014-12-27T00:00:00.000Z")

    def setupSpec() {
        currentTZ = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.forTimeZone(TimeZone.getTimeZone("America/Chicago")))
        timeSeriesQuerySpec = new TimeSeriesQuerySpec()
        pageViews = new LongSumAggregation("pageViewsSum", "pageViews")
        Aggregation lookbackPageViews = new LongSumAggregation("lookback_pageViewsSum", "lookback_pageViews")
        PostAggregation postAggregation1 = new FieldAccessorPostAggregation(pageViews)
        PostAggregation postAggregation2 = new FieldAccessorPostAggregation(lookbackPageViews)
        postAggregation = [new ArithmeticPostAggregation(
                "postAggAdd",
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS,
                [postAggregation1, postAggregation2]
        )]
        timeSeriesQuery = timeSeriesQuerySpec.defaultQuery(aggregations: [pageViews])
        String agg1 = """
                [
                    {
                        "type":"longSum",
                        "name":"pageViewsSum",
                        "fieldName":"pageViews"
                    }
                ]
        """
        timeseriesExpectedString = timeSeriesQuerySpec.stringQuery([aggregations: agg1])
        GroupByQuerySpec groupByQuerySpec = new GroupByQuerySpec()
        groupByQuery = groupByQuerySpec.defaultQuery([:])
        groupByExpectedString = groupByQuerySpec.stringQuery([:])

        intervalsFunctionSetup()
    }

    def intervalsFunctionSetup() {
        Period lookbackOffsetOneday = Period.days(-1)
        Period lookbackOffsetSevendays = Period.days(-7)

        QueryDataSource singleIntervalDataSource = new QueryDataSource(timeSeriesQuerySpec.defaultQuery(intervals: [INTERVAL_2]))
        QueryDataSource multipleIntervalDataSource = new QueryDataSource(timeSeriesQuerySpec.defaultQuery(intervals: [INTERVAL_1, INTERVAL_2]))

        singleIntervalSingleLookbackOffset = defaultQuery(
                dataSource: singleIntervalDataSource,
                lookbackOffsets: [lookbackOffsetOneday]
        )

        singleIntervalMultipleLookbackOffset = defaultQuery(
                dataSource: singleIntervalDataSource,
                lookbackOffsets: [lookbackOffsetOneday, lookbackOffsetSevendays]
        )

        multipleIntervalSingleLookbackOffset = defaultQuery(
                dataSource: multipleIntervalDataSource,
                lookbackOffsets: [lookbackOffsetOneday]
        )

        multipleIntervalMultipleLookbackOffset = defaultQuery(
                dataSource: multipleIntervalDataSource,
                lookbackOffsets: [lookbackOffsetOneday, lookbackOffsetSevendays]
        )
    }

    def shutdownSpec() {
        DateTimeZone.setDefault(currentTZ)
    }

    LookbackQuery defaultQuery(Map vars) {
        vars.dataSource = vars.dataSource ?: new QueryDataSource<>(timeSeriesQuery)
        vars.postAggregations = vars.postAggregations ?: new ArrayList<PostAggregation>()
        QueryContext initial = new QueryContext([(QueryContext.Param.QUERY_ID): "dummy100"], null)
        QueryContext context = vars.context != null ?
                new QueryContext(initial, vars.context as Map).withValue(QueryContext.Param.QUERY_ID, "dummy100") :
                initial
        vars.lookbackOffsets = vars.lookbackOffsets ?: [Period.days(-1)]
        vars.having = vars.having ?: null
        vars.orderBy = vars.orderBy ?: null
        vars.lookbackPrefixes = vars.lookbackPrefixes ?: null

        new LookbackQuery(
                vars.dataSource,
                vars.postAggregations,
                context,
                vars.lookbackOffsets,
                vars.lookbackPrefixes,
                vars.having,
                vars.orderBy
        )
    }

    def stringQuery(Map vars) {
        vars.queryType = vars.queryType ?: "lookback"
        vars.granularity = vars.granularity ?: DAY
        vars.context = vars.context ?
                /{"queryId":"dummy100",$vars.context}/ :
                /{"queryId": "dummy100"}/
        vars.postAggregations = vars.postAggregations ?: "[]"
        vars.lookbackOffsets = vars.lookbackOffsets ?: [""" "P-1D" """]
        String lookback = /"lookbackPrefixes": $vars.lookbackPrefixes,/
        vars.lookbackPrefixes = vars.lookbackPrefixes != null ? lookback : ""

        """
            {
                "queryType":"$vars.queryType",
                "dataSource":$vars.dataSource,
                "context":$vars.context,
                $vars.lookbackPrefixes
                "postAggregations":$vars.postAggregations,
                "lookbackOffsets":$vars.lookbackOffsets
            }
        """
    }

    def "check Lookback query with Timeseries datasource serialization"() {
        LookbackQuery dq1 = defaultQuery(
                postAggregations: postAggregation,
                lookbackPrefixes: ["lookback_days_","lookback_weeks_"],
                lookbackOffsets: [Period.days(-1), Period.weeks(-1)]
        )

        String actualString = MAPPER.writeValueAsString(dq1)

        String dataSrc = (
                """
                {
                    "type":"query",
                    "query": $timeseriesExpectedString
                }
                """
        )

        String postAgg = """
                [
                    {
                        "name":"postAggAdd",
                        "fields":
                            [
                                {
                                    "fieldName":"pageViewsSum",
                                    "type":"fieldAccess"
                                },
                                {
                                    "fieldName":"lookback_pageViewsSum",
                                    "type":"fieldAccess"
                                }
                            ],
                        "type":"arithmetic",
                        "fn":"+"
                    }
                ]
        """

        String lookbackOffsets = '["P-1D", "P-1W"]'

        String lookbackPrefixes = '["lookback_days_", "lookback_weeks_"]'

        String expectedString = stringQuery(
                dataSource: dataSrc,
                postAggregations: postAgg,
                lookbackPrefixes: lookbackPrefixes,
                lookbackOffsets: lookbackOffsets
        )

        expect:
        GroovyTestUtils.compareJson(actualString, expectedString)
    }

    def "check Lookback query with Groupby datasource serialization"() {
        LookbackQuery dq1 = defaultQuery(dataSource: new QueryDataSource<>(groupByQuery))
        String actualString = MAPPER.writeValueAsString(dq1)

        String dataSrc = """
                {
                    "type":"query",
                    "query": $groupByExpectedString
                }
        """

        String expectedString = stringQuery(dataSource: dataSrc)

        expect:
        GroovyTestUtils.compareJson(actualString, expectedString)
    }

    @Unroll
    def "LookbackQueryRequestedIntervalsFunction returns #expectedIntervals for a given #query"() {
        setup:
        LookbackQuery.LookbackQueryRequestedIntervalsFunction lookbackQueryRequestedIntervalsFunction = new LookbackQuery.LookbackQueryRequestedIntervalsFunction()

        expect:
        lookbackQueryRequestedIntervalsFunction.apply(query) == new SimplifiedIntervalList(expectedIntervals)

        where:
        query                                 | expectedIntervals
        singleIntervalSingleLookbackOffset    | [INTERVAL_2, INTERVAL_1]
        singleIntervalMultipleLookbackOffset  | [INTERVAL_2, INTERVAL_1, INTERVAL_3]
        multipleIntervalSingleLookbackOffset  | [INTERVAL_1, INTERVAL_2, INTERVAL_0]
        multipleIntervalMultipleLookbackOffset| [INTERVAL_1, INTERVAL_2, INTERVAL_3, INTERVAL_4, INTERVAL_0]
    }
}
