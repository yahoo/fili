// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MINUTE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.sql.database.Database.ADDED
import static com.yahoo.bard.webservice.sql.database.Database.COMMENT
import static com.yahoo.bard.webservice.sql.database.Database.DELETED
import static com.yahoo.bard.webservice.sql.database.Database.DELTA
import static com.yahoo.bard.webservice.sql.database.Database.IS_NEW
import static com.yahoo.bard.webservice.sql.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.sql.database.Database.PAGE
import static com.yahoo.bard.webservice.sql.database.Database.USER
import static com.yahoo.bard.webservice.sql.database.Database.WIKITICKER
import static com.yahoo.bard.webservice.sql.helper.Aggregator.sum
import static com.yahoo.bard.webservice.sql.helper.Filters.not
import static com.yahoo.bard.webservice.sql.helper.Filters.or
import static com.yahoo.bard.webservice.sql.helper.Filters.search
import static com.yahoo.bard.webservice.sql.helper.Filters.select
import static com.yahoo.bard.webservice.sql.helper.Havings.and
import static com.yahoo.bard.webservice.sql.helper.Havings.equals
import static com.yahoo.bard.webservice.sql.helper.Havings.gt
import static com.yahoo.bard.webservice.sql.helper.Havings.lt
import static com.yahoo.bard.webservice.sql.helper.Intervals.interval
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.END
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.START
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.getDimension
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.getDimensions
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.groupByQuery
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.timeSeriesQuery
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.topNQuery
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
import com.yahoo.bard.webservice.druid.model.query.AbstractDruidAggregationQuery
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.druid.model.query.TopNQuery
import com.yahoo.bard.webservice.sql.database.Database
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

class SqlConverterSpec extends Specification {
    private static final SqlBackedClient sqlBackedClient = new SqlConverter(Database.getDataSource(), new ObjectMapper())
    private static final String TRUE = "TRUE"
    private static final String FALSE = "FALSE"
    private static final String FIRST_COMMENT = "added project"
    //this is the first result in the database
    private static final String UNIQUE_COMMENT = "took out (then), added quotation marks"
    private static final DruidResponseParser RESPONSE_PARSER = new DruidResponseParser();

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
                DateTimeZone.UTC
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
        );
    }

    private static TimeSeriesQuery getTimeSeriesQuery(DefaultTimeGrain timeGrain, Filter filter) {
        return timeSeriesQuery(
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
                asList(interval(START, END))
        );
    }

    private static TopNQuery getTopNQuery(
            DefaultTimeGrain timeGrain,
            Filter filter,
            String orderMetric,
            String dimension,
            int threshold
    ) {
        return topNQuery(
                WIKITICKER,
                filter,
                threshold,
                orderMetric,
                getDimension(dimension),
                timeGrain,
                asList(ADDED),
                asList(dimension),
                asList(sum(ADDED)),
                asList(),
                asList(interval(START, END))
        );
    }

    @Unroll
    def "ExecuteQuery for #timeGrain want #size filter on #filter"() {
        expect:
        DruidQuery druidQuery = getTimeSeriesQuery(timeGrain, filter)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get();
        ResultSet parse = parse(jsonNode, druidQuery)

        parse.size() == size

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
    def "Test timeseries on /#timeGrain/"() {
        setup:
        DruidQuery druidQuery = getBasicTimeseriesQuery(timeGrain)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get();
        ResultSet parse = parse(jsonNode, druidQuery)

        expect:
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
        WEEK      | null                            | """[{"timestamp":"2015-09-10T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        MONTH     | null                            | """[{"timestamp":"2015-09-01T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
        YEAR      | null                            | """[{"timestamp":"2015-01-01T00:00:00.000Z","event":{"${ADDED}":9385573.0,"${DELETED}":394298.0,"${DELTA}":8991275.0}}]"""
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

    @Unroll
    def "Test topN on /#timeGrain/#dimension/?metrics=#metric&topN=#threshold"() {
        setup:
        DruidQuery druidQuery = getTopNQuery(timeGrain, filter, metric, dimension, threshold)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery, null, null).get();

        expect:
        ResultSet parse = parse(jsonNode, druidQuery)
        parse.size() <= size

        where: "we have"
        timeGrain | filter | metric | dimension | threshold | size
        DAY       | null   | ADDED  | PAGE      | 1         | 1
        DAY       | null   | ADDED  | PAGE      | 10        | 10
        HOUR      | null   | ADDED  | PAGE      | 1         | 24
        HOUR      | null   | ADDED  | USER      | 10        | 24 * 10
        MINUTE    | null   | ADDED  | USER      | 10        | 24 * 60 * 10
    }
}
