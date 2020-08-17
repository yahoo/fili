// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MINUTE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.database.Database.ADDED
import static com.yahoo.bard.webservice.database.Database.COMMENT
import static com.yahoo.bard.webservice.database.Database.COUNTRY_ISO_CODE
import static com.yahoo.bard.webservice.database.Database.DELETED
import static com.yahoo.bard.webservice.database.Database.DELTA
import static com.yahoo.bard.webservice.database.Database.IS_NEW
import static com.yahoo.bard.webservice.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.database.Database.METRO_CODE
import static com.yahoo.bard.webservice.database.Database.PAGE
import static com.yahoo.bard.webservice.database.Database.REGION_ISO_CODE
import static com.yahoo.bard.webservice.database.Database.USER
import static com.yahoo.bard.webservice.database.Database.WIKITICKER
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.ASC
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.DESC
import static com.yahoo.bard.webservice.data.time.AllGranularity.INSTANCE
import static com.yahoo.bard.webservice.sql.builders.Aggregator.longMax
import static com.yahoo.bard.webservice.sql.builders.Aggregator.longMin
import static com.yahoo.bard.webservice.sql.builders.Aggregator.longSum
import static com.yahoo.bard.webservice.sql.builders.Aggregator.max
import static com.yahoo.bard.webservice.sql.builders.Aggregator.min
import static com.yahoo.bard.webservice.sql.builders.Aggregator.sum
import static com.yahoo.bard.webservice.sql.builders.Filters.not
import static com.yahoo.bard.webservice.sql.builders.Filters.or
import static com.yahoo.bard.webservice.sql.builders.Filters.search
import static com.yahoo.bard.webservice.sql.builders.Filters.select
import static com.yahoo.bard.webservice.sql.builders.Havings.and
import static com.yahoo.bard.webservice.sql.builders.Havings.equal
import static com.yahoo.bard.webservice.sql.builders.Havings.gt
import static com.yahoo.bard.webservice.sql.builders.Havings.lt
import static com.yahoo.bard.webservice.sql.builders.Intervals.interval
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.END
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.START
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.dataSource
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getDimension
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getDimensions
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.groupByQuery
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.timeSeriesQuery
import static com.yahoo.bard.webservice.sql.DruidQueryToSqlConverterSpec.API_PREPEND

import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.database.Database
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.table.Column

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Function

class DefaultSqlBackedClientSpec extends Specification {
    private static SqlBackedClient sqlBackedClient = new DefaultSqlBackedClient(Database.getDataSource(), new ObjectMapper())
    private static final String TRUE = "TRUE"
    private static final String FALSE = "FALSE"
    private static final String FIRST_COMMENT = "added project"
    // FIRST_COMMENT is the first result in the database
    private static final String UNIQUE_COMMENT = "took out (then), added quotation marks"
    private static final DruidResponseParser RESPONSE_PARSER = new DruidResponseParser()

    ResultSet parse(JsonNode jsonNode, AbstractDruidAggregationQuery<?> druidQuery) {
        List<Column> columns = new ArrayList<>()
        def constraint = druidQuery.dataSource.physicalTable.constraint
        constraint.metricNames.forEach { columns.add(new MetricColumn(it)) }
        constraint.allDimensionNames.forEach { columns.add(new DimensionColumn(getDimension(it))) }

        ResultSetSchema resultSetSchema = new ResultSetSchema(druidQuery.granularity, columns)
        return RESPONSE_PARSER.parse(
                jsonNode,
                resultSetSchema,
                DefaultQueryType.GROUP_BY,
                druidQuery.dataSource.physicalTable.schema.timeGrain.timeZone
        )
    }

    private static TimeSeriesQuery getTimeSeriesQuery(DefaultTimeGrain timeGrain, Filter filter) {
        return getTimeSeriesQueryCustomAggregation(timeGrain, filter, { s -> sum(s) }, [])
    }

    private static TimeSeriesQuery getTimeSeriesQueryCustomAggregation(DefaultTimeGrain timeGrain, Filter filter, Function<String, Aggregation> aggregation, List<PostAggregation> postAggregations) {
        return timeSeriesQuery(
                WIKITICKER,
                filter,
                timeGrain,
                [ADDED, DELETED, DELTA],
                [COMMENT],
                [aggregation.apply(ADDED), aggregation.apply(DELETED), aggregation.apply(DELTA)],
                postAggregations,
                [interval(START, END)]
        )
    }

    private static TimeSeriesQuery getTimeSeriesQueryMultipleIntervals(DefaultTimeGrain timeGrain, Filter filter) {
        return timeSeriesQuery(
                WIKITICKER,
                filter,
                timeGrain,
                [ADDED, DELETED, DELTA],
                [COMMENT],
                [sum(ADDED), sum(DELETED), sum(DELTA)],
                [],
                [interval(START, "2015-09-12T12:00:00.000Z"), interval("2015-09-12T12:00:00.000Z", END)]
        )
    }


    private static GroupByQuery getGroupByQuery(
            Granularity timeGrain,
            Filter filter,
            Having having,
            List<String> dimensions,
            LimitSpec limitSpec
    ) {
        return groupByQuery(
                WIKITICKER,
                filter,
                having,
                getDimensions(dimensions),
                timeGrain,
                [ADDED, DELETED],
                [COMMENT],
                [sum(ADDED), sum(DELETED)],
                [],
                [interval(START, END)],
                limitSpec
        )
    }

    @Unroll
    def "ExecuteQuery for #timeGrain want #size filter on #filter"() {
        setup:
        DruidQuery druidQueryMultipleIntervals = getTimeSeriesQueryMultipleIntervals(timeGrain, filter)
        JsonNode jsonNodeMultipleIntervals = sqlBackedClient.executeQuery(druidQueryMultipleIntervals, null, null).get()
        ResultSet parseMultipleIntervals = parse(jsonNodeMultipleIntervals, druidQueryMultipleIntervals)

        DruidQuery druidQueryOneInterval = getTimeSeriesQuery(timeGrain, filter)
        JsonNode jsonNodeOneInterval = sqlBackedClient.executeQuery(druidQueryOneInterval, null, null).get()
        ResultSet parseOneInterval = parse(jsonNodeOneInterval, druidQueryOneInterval)

        expect:
        parseOneInterval.size() == size
        parseMultipleIntervals.size() == size

        where: "we have"
        timeGrain | filter                                              | size
        MINUTE    | null                                                | 1394
        MINUTE    | search(COMMENT, FIRST_COMMENT)                      | 1
        MINUTE    | select(COMMENT, FIRST_COMMENT)                      | 1
        MINUTE    | not(select(COMMENT, FIRST_COMMENT))                 | 1393
        MINUTE    | or(select(IS_ROBOT, TRUE), select(IS_ROBOT, FALSE)) | 1394
        HOUR      | null                                                | 24
        DAY       | null                                                | 1
        WEEK      | null                                                | 1
        MONTH     | null                                                | 1
        YEAR      | null                                                | 1
    }

    @Unroll
    def "Test timeseries query on #timeGrain for #timeZone"() {
        setup:
        def timeZoneId = DateTimeZone.forID(timeZone)
        // shift the start and end dates by the offset from utc time
        def raw = new DateTime(START)
        def start = new DateTime(START).plusMillis(-timeZoneId.getOffset(raw)).toString()
        def end = new DateTime(END).plusMillis(-timeZoneId.getOffset(raw)).toString()

        TimeSeriesQuery timeSeriesQuery = new TimeSeriesQuery(
                dataSource(WIKITICKER, DAY, timeZoneId, [ADDED], [], "", ""),
                timeGrain,
                null,
                [sum(ADDED)],
                [],
                [interval(start, end)]
        )
        JsonNode jsonNode = sqlBackedClient.executeQuery(timeSeriesQuery, null, null).get()
        ResultSet parse = parse(jsonNode, timeSeriesQuery)

        expect:
        parse.size() == size
        parse.get(0).getTimeStamp().toDateTime(timeZoneId).toString().contains(parsedResultText)
        jsonNode.get(0).toString().contains(druidResultText)

        where:
        timeZone          | timeGrain | size | parsedResultText          | druidResultText
        "America/Chicago" | MINUTE    | 1394 | "2015-09-12T00:46:00.000" | "2015-09-12T05:46:00.000Z"
        "America/Chicago" | HOUR      | 24   | "2015-09-12T00:00:00.000" | "2015-09-12T05:00:00.000Z"
        "America/Chicago" | DAY       | 1    | "2015-09-12T00:00:00.000" | "2015-09-12T05:00:00.000Z"
        "America/Chicago" | WEEK      | 1    | "2015-09-07T00:00:00.000" | "2015-09-07T05:00:00.000Z"
        "America/Chicago" | MONTH     | 1    | "2015-09-01T00:00:00.000" | "2015-09-01T05:00:00.000Z"
        "America/Chicago" | YEAR      | 1    | "2015-01-01T00:00:00.000" | "2015-01-01T06:00:00.000Z"
        "UTC"             | YEAR      | 1    | "2015-01-01T00:00:00.000" | "2015-01-01T00:00:00.000Z"
    }

    @Unroll
    def "Test timeseries on /#timeGrain/ with #filter"() {
        setup:
        DruidQuery druidQuery = getTimeSeriesQuery(timeGrain, filter)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get()

        expect:
        parse(jsonNode, druidQuery)
        jsonNode.toString() == response

        where: "we have"
        timeGrain | filter                          | response
        HOUR      | select(COMMENT, FIRST_COMMENT)  | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":36.0,"${API_PREPEND}${DELETED}":0.0,"${API_PREPEND}${DELTA}":36.0}}]"""
        HOUR      | select(COMMENT, UNIQUE_COMMENT) | """[{"timestamp":"2015-09-12T01:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0.0,"${API_PREPEND}${DELETED}":5.0,"${API_PREPEND}${DELTA}":-5.0}}]"""
        DAY       | null                            | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0}}]"""
        WEEK      | null                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0}}]"""
        MONTH     | null                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0}}]"""
        YEAR      | null                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0}}]"""
    }

    @Unroll
    def "Test doubles/longs serialize correctly on /#timeGrain/ with custom aggregation"() {
        setup:
        DruidQuery druidQuery = getTimeSeriesQueryCustomAggregation(timeGrain, null, aggregation, postAgg)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get()

        expect:
        parse(jsonNode, druidQuery)
        jsonNode.toString() == response

        where: "we have"
        timeGrain | aggregation         | postAgg                                                                                                                                                                                                                       | response
        DAY       | { s -> sum(s) }     | [new ArithmeticPostAggregation("added_to_deleted_ratio", ArithmeticPostAggregation.ArithmeticPostAggregationFunction.DIVIDE, [new FieldAccessorPostAggregation(sum(ADDED)), new FieldAccessorPostAggregation(sum(DELETED))])] | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0,"added_to_deleted_ratio":"23.80324779735124195405505481"}}]"""
        WEEK      | { s -> sum(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0}}]"""
        MONTH     | { s -> sum(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0}}]"""
        YEAR      | { s -> sum(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573.0,"${API_PREPEND}${DELETED}":394298.0,"${API_PREPEND}${DELTA}":8991275.0}}]"""
        DAY       | { s -> min(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0.0,"${API_PREPEND}${DELETED}":0.0,"${API_PREPEND}${DELTA}":-500.0}}]"""
        WEEK      | { s -> min(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0.0,"${API_PREPEND}${DELETED}":0.0,"${API_PREPEND}${DELTA}":-500.0}}]"""
        MONTH     | { s -> min(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0.0,"${API_PREPEND}${DELETED}":0.0,"${API_PREPEND}${DELTA}":-500.0}}]"""
        YEAR      | { s -> min(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0.0,"${API_PREPEND}${DELETED}":0.0,"${API_PREPEND}${DELTA}":-500.0}}]"""
        DAY       | { s -> max(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818.0,"${API_PREPEND}${DELETED}":500.0,"${API_PREPEND}${DELTA}":199818.0}}]"""
        WEEK      | { s -> max(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818.0,"${API_PREPEND}${DELETED}":500.0,"${API_PREPEND}${DELTA}":199818.0}}]"""
        MONTH     | { s -> max(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818.0,"${API_PREPEND}${DELETED}":500.0,"${API_PREPEND}${DELTA}":199818.0}}]"""
        YEAR      | { s -> max(s) }     | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818.0,"${API_PREPEND}${DELETED}":500.0,"${API_PREPEND}${DELTA}":199818.0}}]"""
        DAY       | { s -> longSum(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573,"${API_PREPEND}${DELETED}":394298,"${API_PREPEND}${DELTA}":8991275}}]"""
        WEEK      | { s -> longSum(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573,"${API_PREPEND}${DELETED}":394298,"${API_PREPEND}${DELTA}":8991275}}]"""
        MONTH     | { s -> longSum(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573,"${API_PREPEND}${DELETED}":394298,"${API_PREPEND}${DELTA}":8991275}}]"""
        YEAR      | { s -> longSum(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":9385573,"${API_PREPEND}${DELETED}":394298,"${API_PREPEND}${DELTA}":8991275}}]"""
        DAY       | { s -> longMin(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0,"${API_PREPEND}${DELETED}":0,"${API_PREPEND}${DELTA}":-500}}]"""
        WEEK      | { s -> longMin(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0,"${API_PREPEND}${DELETED}":0,"${API_PREPEND}${DELTA}":-500}}]"""
        MONTH     | { s -> longMin(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0,"${API_PREPEND}${DELETED}":0,"${API_PREPEND}${DELTA}":-500}}]"""
        YEAR      | { s -> longMin(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":0,"${API_PREPEND}${DELETED}":0,"${API_PREPEND}${DELTA}":-500}}]"""
        DAY       | { s -> longMax(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818,"${API_PREPEND}${DELETED}":500,"${API_PREPEND}${DELTA}":199818}}]"""
        WEEK      | { s -> longMax(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818,"${API_PREPEND}${DELETED}":500,"${API_PREPEND}${DELTA}":199818}}]"""
        MONTH     | { s -> longMax(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818,"${API_PREPEND}${DELETED}":500,"${API_PREPEND}${DELTA}":199818}}]"""
        YEAR      | { s -> longMax(s) } | []                                                                                                                                                                                                                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${API_PREPEND}${ADDED}":199818,"${API_PREPEND}${DELETED}":500,"${API_PREPEND}${DELTA}":199818}}]"""

    }

    @Unroll
    def "Test groupBy on /#timeGrain/#dims/"() {
        setup:
        DruidQuery druidQuery = getGroupByQuery(timeGrain, filter, having, dims, null)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get()

        expect:
        ResultSet parse = parse(jsonNode, druidQuery)
        parse.size() == size

        where: "we have"
        timeGrain | dims               | filter                         | having                                                         | size
        INSTANCE  | []                 | null                           | null                                                           | 39244
        HOUR      | [IS_NEW, IS_ROBOT] | null                           | and(gt(API_PREPEND + ADDED, 1), lt(API_PREPEND + ADDED, 1.01)) | 0
        HOUR      | [IS_ROBOT]         | null                           | null                                                           | 24 * 2
        DAY       | [IS_NEW, IS_ROBOT] | null                           | null                                                           | 4
        HOUR      | [IS_NEW, IS_ROBOT] | null                           | equal(API_PREPEND + ADDED, 0)                                  | 0
        HOUR      | [IS_NEW, IS_ROBOT] | search(COMMENT, FIRST_COMMENT) | equal(API_PREPEND + ADDED, 36)                                 | 1
        HOUR      | []                 | null                           | gt(API_PREPEND + ADDED, 400000)                                | 12
        HOUR      | []                 | null                           | null                                                           | 24
        DAY       | [PAGE, USER]       | null                           | null                                                           | 36565
        DAY       | []                 | null                           | null                                                           | 1
    }

    @Unroll
    def "test sorting on #dims with #metrics by #metricDirections expecting #expectedSize results"() {
        setup:
        DruidQuery druidQuery = getGroupByQuery(grain, null, null, dims, SimpleDruidQueryBuilder.getSort(metrics, metricDirections, limit))
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get()

        expect:
        ResultSet parse = parse(jsonNode, druidQuery)
        parse.size() == expectedSize

        where:
        grain    | dims                                    | metrics                                      | metricDirections | limit            | expectedSize
        DAY      | [METRO_CODE]                            | [API_PREPEND + ADDED]                        | [DESC]           | Optional.of(10)  | 10
        DAY      | []                                      | [API_PREPEND + ADDED, API_PREPEND + DELETED] | [DESC, ASC]      | Optional.of(10)  | 1
        DAY      | [METRO_CODE, IS_ROBOT, REGION_ISO_CODE] | [API_PREPEND + ADDED]                        | [DESC]           | Optional.of(100) | 100
        YEAR     | [METRO_CODE]                            | []                                           | []               | Optional.of(1)   | 1
        MONTH    | [USER]                                  | []                                           | []               | Optional.of(49)  | 49
        INSTANCE | [COUNTRY_ISO_CODE]                      | []                                           | []               | Optional.of(25)  | 25
    }
}
