// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MINUTE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.helper.Aggregator.sum
import static com.yahoo.bard.webservice.helper.Filters.not
import static com.yahoo.bard.webservice.helper.Filters.or
import static com.yahoo.bard.webservice.helper.Filters.search
import static com.yahoo.bard.webservice.helper.Filters.select
import static com.yahoo.bard.webservice.helper.Havings.and
import static com.yahoo.bard.webservice.helper.Havings.equals
import static com.yahoo.bard.webservice.helper.Havings.gt
import static com.yahoo.bard.webservice.helper.Havings.lt
import static com.yahoo.bard.webservice.helper.Intervals.interval
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.ADDED
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.COMMENT
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.DELETED
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.DELTA
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.END
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.IS_NEW
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.IS_ROBOT
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.START
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.WIKITICKER
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.getDimension
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.getDimensions
import static java.util.Arrays.asList

import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.test.Database

import com.fasterxml.jackson.databind.JsonNode

import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Stream

class SqlConverterSpec extends Specification {
    private static final SqlBackedClient sqlBackedClient = new SqlConverter(
            Database.getDatabase(),
            Database.getDataSource()
    )
    private static final String TRUE = "TRUE"
    private static final String FALSE = "FALSE"

    static ResultSet parse(DefaultTimeGrain timeGrain, JsonNode jsonNode, DefaultQueryType queryType) {
        DruidResponseParser druidResponseParser = new DruidResponseParser()

        List<Column> columns = new ArrayList<>()
        Stream.of(ADDED, DELETED, DELTA)
                .map { new MetricColumn(it) }
                .forEach { columns.add(it) }
        Stream.of(COMMENT)
                .map { new DimensionColumn(getDimension(it)) }
                .forEach { columns.add(it) }

        ResultSetSchema resultSetSchema = new ResultSetSchema(timeGrain, columns)
        return druidResponseParser.parse(
                jsonNode,
                resultSetSchema,
                queryType,
                DateTimeZone.UTC
        )
    }

    private static TimeSeriesQuery getBasicTimeseriesQuery(DefaultTimeGrain timeGrain) {
        return SimpleDruidQueryBuilder.timeSeriesQuery(
                WIKITICKER,
                null,
                timeGrain,
                asList(ADDED),
                asList(),
                asList(sum(ADDED)),
                asList(),
                asList(interval(START, END))
        );
    }

    private static TimeSeriesQuery getTimeSeriesQuery(DefaultTimeGrain timeGrain, Filter filter) {
        return SimpleDruidQueryBuilder.timeSeriesQuery(
                WIKITICKER,
                filter,
                timeGrain,
                asList(ADDED, DELETED, DELTA),
                asList(COMMENT),
                asList(sum(ADDED), sum(DELETED), sum(DELTA)),
                asList(),
                asList(interval(START, END))
        );
    }

    private static GroupByQuery getGroupByQuery(
            DefaultTimeGrain timeGrain,
            Filter filter,
            Having having,
            List<String> dimensions
    ) {
        return SimpleDruidQueryBuilder.groupByQuery(
                WIKITICKER,
                filter,
                having,
                getDimensions(dimensions),
                timeGrain,
                asList(ADDED, DELETED, DELTA),
                asList(COMMENT),
                asList(sum(ADDED)),
                asList(),
                asList(interval(START, END))
        );
    }

    @Unroll
    def "ExecuteQuery for #timeGrain want #size filter on #filter"() {
        expect:
        DruidQuery druidQuery = getTimeSeriesQuery(timeGrain, filter)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery).get();
        ResultSet parse = parse(timeGrain, jsonNode, DefaultQueryType.TIMESERIES)

        parse.size() == size

        where: "we have"
        timeGrain | filter                                              | size
        MINUTE    | null                                                | 1394
        MINUTE    | search(COMMENT, "added project")                    | 1
        MINUTE    | select(COMMENT, "added project")                    | 1
        MINUTE    | not(select(COMMENT, "added project"))               | 1393
        MINUTE    | or(select(IS_ROBOT, TRUE), select(IS_ROBOT, FALSE)) | 1394
        // MINUTE    | and(select(IS_ROBOT, TRUE), not(select(IS_ROBOT, TRUE))) | 0
        // TODO: why does this fail. It may be that Calcite doesn't let you do something like this
        HOUR      | null                                                | 24
        DAY       | null                                                | 1
        WEEK      | null                                                | 1
        MONTH     | null                                                | 1
        YEAR      | null                                                | 1

    }

    @Unroll
    def "ExecuteQuery for #timeGrain with basic timeseries"() {
        expect:
        DruidQuery druidQuery = getBasicTimeseriesQuery(timeGrain)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery).get();
        ResultSet parse = parse(timeGrain, jsonNode, DefaultQueryType.TIMESERIES)

        parse.size() == size

        where: "we have"
        timeGrain | size
        MINUTE    | 1394
        HOUR      | 24
        DAY       | 1
        WEEK      | 1
        MONTH     | 1
        YEAR      | 1

    }

    @Unroll
    def "Test response output and parsing for #timeGrain"() {
        setup:
        DruidQuery druidQuery = getTimeSeriesQuery(timeGrain, filter)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery).get();

        expect:
        parse(timeGrain, jsonNode, DefaultQueryType.TIMESERIES)
        jsonNode.toString() == response

        where: "we have"
        timeGrain | filter | response
        HOUR      | search(COMMENT, "added project")   | """[{"timestamp":"2015-09-12T00:00:00.000Z","result":{"ADDED":36.0,"DELTA":36.0,"DELETED":0.0}}]"""
        HOUR      | search(COMMENT, "took out (then), added quotation marks")   | """[{"timestamp":"2015-09-12T01:00:00.000Z","result":{"ADDED":0.0,"DELTA":-5.0,"DELETED":5.0}}]"""
        DAY       | null   | """[{"timestamp":"2015-09-12T00:00:00.000Z","result":{"ADDED":9385573.0,"DELTA":8991275.0,"DELETED":394298.0}}]"""
        WEEK      | null   | """[{"timestamp":"2015-09-10T00:00:00.000Z","result":{"ADDED":9385573.0,"DELTA":8991275.0,"DELETED":394298.0}}]"""
        MONTH     | null   | """[{"timestamp":"2015-09-01T00:00:00.000Z","result":{"ADDED":9385573.0,"DELTA":8991275.0,"DELETED":394298.0}}]"""
        YEAR      | null   | """[{"timestamp":"2015-01-01T00:00:00.000Z","result":{"ADDED":9385573.0,"DELTA":8991275.0,"DELETED":394298.0}}]"""
    }

    @Unroll
    def "Test handling of groupBy queries"() {
        setup:
        DruidQuery druidQuery = getGroupByQuery(timeGrain, filter, having, dims)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery).get();

        expect:
        ResultSet parse = parse(timeGrain, jsonNode, DefaultQueryType.GROUP_BY)
        parse.size() == size

        where: "we have"
        timeGrain | dims                     | filter                           | having                          | size
        HOUR      | asList(IS_NEW, IS_ROBOT) | null                             | and(gt(ADDED, 1), lt(ADDED, 1)) | 0
        HOUR      | asList(IS_NEW, IS_ROBOT) | null                             | equals(ADDED, 0)                | 0
        HOUR      | asList(IS_NEW, IS_ROBOT) | search(COMMENT, "added project") | equals(ADDED, 36)               | 1
        HOUR      | asList()                 | null                             | gt(ADDED, 400000)               | 12
        HOUR      | asList()                 | null                             | null                            | 24

    }
}
