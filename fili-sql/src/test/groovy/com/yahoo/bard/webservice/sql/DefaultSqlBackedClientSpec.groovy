// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MINUTE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.druid.model.query.AllGranularity.INSTANCE
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
import static com.yahoo.bard.webservice.sql.builders.Havings.equals
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
import static com.yahoo.bard.webservice.database.Database.ADDED
import static com.yahoo.bard.webservice.database.Database.COMMENT
import static com.yahoo.bard.webservice.database.Database.DELETED
import static com.yahoo.bard.webservice.database.Database.DELTA
import static com.yahoo.bard.webservice.database.Database.IS_NEW
import static com.yahoo.bard.webservice.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.database.Database.PAGE
import static com.yahoo.bard.webservice.database.Database.USER
import static com.yahoo.bard.webservice.database.Database.WIKITICKER
import static java.util.Arrays.asList

import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.database.Database
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

    private static TimeSeriesQuery getBasicTimeseriesQuery(DefaultTimeGrain timeGrain) {
        return timeSeriesQuery(
                WIKITICKER,
                null,
                timeGrain,
                asList(ADDED),
                asList(),
                asList(sum(ADDED)),
                asList(),
                asList(interval(START, END))
        )
    }

    private static TimeSeriesQuery getTimeSeriesQuery(DefaultTimeGrain timeGrain, Filter filter) {
        return getTimeSeriesQueryCustomAggregation(timeGrain, filter, { s -> sum(s) })
    }

    private static TimeSeriesQuery getTimeSeriesQueryCustomAggregation(DefaultTimeGrain timeGrain, Filter filter, Function<String, Aggregation> aggregation) {
        return timeSeriesQuery(
                WIKITICKER,
                filter,
                timeGrain,
                asList(ADDED, DELETED, DELTA),
                asList(COMMENT),
                asList(aggregation.apply(ADDED), aggregation.apply(DELETED), aggregation.apply(DELTA)),
                asList(),
                asList(interval(START, END))
        )
    }

    private static TimeSeriesQuery getTimeSeriesQueryMultipleIntervals(DefaultTimeGrain timeGrain, Filter filter) {
        return timeSeriesQuery(
                WIKITICKER,
                filter,
                timeGrain,
                asList(ADDED, DELETED, DELTA),
                asList(COMMENT),
                asList(sum(ADDED), sum(DELETED), sum(DELTA)),
                asList(),
                asList(interval(START, "2015-09-12T12:00:00.000Z"), interval("2015-09-12T12:00:00.000Z", END))
        )
    }


    private static GroupByQuery getGroupByQuery(
            Granularity timeGrain,
            Filter filter,
            Having having,
            List<String> dimensions
    ) {
        return groupByQuery(
                WIKITICKER,
                filter,
                having,
                getDimensions(dimensions),
                timeGrain,
                asList(ADDED, DELETED),
                asList(COMMENT),
                asList(sum(ADDED), sum(DELETED)),
                asList(),
                asList(interval(START, END)),
                null
        )
    }

    @Unroll
    def "ExecuteQuery for #timeGrain want #size filter on #filter"() {
        setup:
        DruidQuery druidQueryMultipleIntervals = getTimeSeriesQueryMultipleIntervals(timeGrain, filter)
        JsonNode jsonNodeMultipleIntervals = sqlBackedClient.executeQuery(druidQueryMultipleIntervals, null, null).get();
        ResultSet parseMultipleIntervals = parse(jsonNodeMultipleIntervals, druidQueryMultipleIntervals)

        DruidQuery druidQueryOneInterval = getTimeSeriesQuery(timeGrain, filter)
        JsonNode jsonNodeOneInterval = sqlBackedClient.executeQuery(druidQueryOneInterval, null, null).get();
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
        def start = new DateTime(START).plusMillis(-timeZoneId.getOffset(new DateTime(DateTimeZone.UTC))).toString()
        def end = new DateTime(END).plusMillis(-timeZoneId.getOffset(new DateTime(DateTimeZone.UTC))).toString()

        TimeSeriesQuery timeSeriesQuery = new TimeSeriesQuery(
                dataSource(WIKITICKER, DAY, timeZoneId, asList(ADDED), asList(), "", ""),
                timeGrain,
                null,
                asList(sum(ADDED)),
                Collections.emptyList(),
                asList(interval(start, end))
        )
        JsonNode jsonNode = sqlBackedClient.executeQuery(timeSeriesQuery, null, null).get();
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
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get();

        expect:
        parse(jsonNode, druidQuery)
        jsonNode.toString() == response

        where: "we have"
        timeGrain | filter                          | response
        HOUR      | select(COMMENT, FIRST_COMMENT)  | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":36.0,"${DELETED}":0.0,"${DELTA}":36.0}}]"""
        HOUR      | select(COMMENT, UNIQUE_COMMENT) | """[{"timestamp":"2015-09-12T01:00:00.000Z","event":{"${ADDED}":0.0,"${DELETED}":5.0,"${DELTA}":-5.0}}]"""
        DAY       | null                            | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        WEEK      | null                            | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        MONTH     | null                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        YEAR      | null                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
    }

    @Unroll
    def "Test doubles/longs serialize correctly on /#timeGrain/ with custom aggregation"() {
        setup:
        DruidQuery druidQuery = getTimeSeriesQueryCustomAggregation(timeGrain, null, aggregation)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get();

        expect:
        parse(jsonNode, druidQuery)
        jsonNode.toString() == response

        where: "we have"
        timeGrain | aggregation         | response
        DAY       | { s -> sum(s) }     | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        WEEK      | { s -> sum(s) }     | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        MONTH     | { s -> sum(s) }     | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        YEAR      | { s -> sum(s) }     | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        DAY       | { s -> min(s) }     | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":0.0,"${DELETED}":0.0,"${DELTA}":-500.0}}]"""
        WEEK      | { s -> min(s) }     | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${ADDED}":0.0,"${DELETED}":0.0,"${DELTA}":-500.0}}]"""
        MONTH     | { s -> min(s) }     | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":0.0,"${DELETED}":0.0,"${DELTA}":-500.0}}]"""
        YEAR      | { s -> min(s) }     | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":0.0,"${DELETED}":0.0,"${DELTA}":-500.0}}]"""
        DAY       | { s -> max(s) }     | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":199818.0,"${DELETED}":500.0,"${DELTA}":199818.0}}]"""
        WEEK      | { s -> max(s) }     | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${ADDED}":199818.0,"${DELETED}":500.0,"${DELTA}":199818.0}}]"""
        MONTH     | { s -> max(s) }     | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":199818.0,"${DELETED}":500.0,"${DELTA}":199818.0}}]"""
        YEAR      | { s -> max(s) }     | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":199818.0,"${DELETED}":500.0,"${DELTA}":199818.0}}]"""
        DAY       | { s -> longSum(s) } | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":9385573,"${DELETED}":394298,"${DELTA}":8991275}}]"""
        WEEK      | { s -> longSum(s) } | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${ADDED}":9385573,"${DELETED}":394298,"${DELTA}":8991275}}]"""
        MONTH     | { s -> longSum(s) } | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":9385573,"${DELETED}":394298,"${DELTA}":8991275}}]"""
        YEAR      | { s -> longSum(s) } | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":9385573,"${DELETED}":394298,"${DELTA}":8991275}}]"""
        DAY       | { s -> longMin(s) } | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":0,"${DELETED}":0,"${DELTA}":-500}}]"""
        WEEK      | { s -> longMin(s) } | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${ADDED}":0,"${DELETED}":0,"${DELTA}":-500}}]"""
        MONTH     | { s -> longMin(s) } | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":0,"${DELETED}":0,"${DELTA}":-500}}]"""
        YEAR      | { s -> longMin(s) } | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":0,"${DELETED}":0,"${DELTA}":-500}}]"""
        DAY       | { s -> longMax(s) } | """[{"timestamp":"2015-09-12T00:00:00.000Z","event":{"${ADDED}":199818,"${DELETED}":500,"${DELTA}":199818}}]"""
        WEEK      | { s -> longMax(s) } | """[{"timestamp":"2015-09-07T00:00:00.000Z","event":{"${ADDED}":199818,"${DELETED}":500,"${DELTA}":199818}}]"""
        MONTH     | { s -> longMax(s) } | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":199818,"${DELETED}":500,"${DELTA}":199818}}]"""
        YEAR      | { s -> longMax(s) } | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":199818,"${DELETED}":500,"${DELTA}":199818}}]"""

    }

    @Unroll
    def "Test groupBy on /#timeGrain/#dims/"() {
        setup:
        DruidQuery druidQuery = getGroupByQuery(timeGrain, filter, having, dims)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get();

        expect:
        ResultSet parse = parse(jsonNode, druidQuery)
        parse.size() == size

        where: "we have"
        timeGrain | dims                     | filter                         | having                          | size
        INSTANCE  | asList()                 | null                           | null                            | 39244
        HOUR      | asList(IS_NEW, IS_ROBOT) | null                           | and(gt(ADDED, 1), lt(ADDED, 1)) | 0
        HOUR      | asList(IS_ROBOT)         | null                           | null                            | 24 * 2
        DAY       | asList(IS_NEW, IS_ROBOT) | null                           | null                            | 4
        HOUR      | asList(IS_NEW, IS_ROBOT) | null                           | equals(ADDED, 0)                | 0
        HOUR      | asList(IS_NEW, IS_ROBOT) | search(COMMENT, FIRST_COMMENT) | equals(ADDED, 36)               | 1
        HOUR      | asList()                 | null                           | gt(ADDED, 400000)               | 12
        HOUR      | asList()                 | null                           | null                            | 24
        DAY       | asList(PAGE, USER)       | null                           | null                            | 36565
        DAY       | asList()                 | null                           | null                            | 1
    }
}
