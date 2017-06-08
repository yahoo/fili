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
import static com.yahoo.bard.webservice.helper.Filters.and
import static com.yahoo.bard.webservice.helper.Filters.not
import static com.yahoo.bard.webservice.helper.Filters.or
import static com.yahoo.bard.webservice.helper.Filters.search
import static com.yahoo.bard.webservice.helper.Filters.select
import static com.yahoo.bard.webservice.helper.Intervals.interval
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.ADDED
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.COMMENT
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.DELETED
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.DELTA
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.END
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.IS_ROBOT
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.START
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.WIKITICKER
import static java.util.Arrays.asList

import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.test.Database

import com.fasterxml.jackson.databind.JsonNode

import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Stream

/**
 * Created by hinterlong on 6/7/17.
 */
class SQLConverterSpec extends Specification {
    private static final SqlBackedClient sqlBackedClient = new SQLConverter(
            Database.getDatabase(),
            Database.getDataSource()
    )
    private static final String TRUE = "TRUE"
    private static final String FALSE = "FALSE"

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

    @Unroll
    def "ExecuteQuery for #timegrain want #size filter on #filter"() {
        expect:
        DruidQuery druidQuery = getTimeSeriesQuery(timegrain, filter)
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery).get();

        DruidResponseParser druidResponseParser = new DruidResponseParser();

        List<Column> columns = new ArrayList<>();
        Stream.of(ADDED, DELETED, DELETED)
                .map { new MetricColumn(it) }
                .forEach { columns.add(it) }

        ResultSetSchema resultSetSchema = new ResultSetSchema(timegrain, columns);
        ResultSet parse = druidResponseParser.parse(
                jsonNode,
                resultSetSchema,
                DefaultQueryType.TIMESERIES,
                DateTimeZone.UTC
        );

        parse.size() == size

        where: "we have"
        timegrain | filter                                               | size
        MINUTE    | null                                                 | 1394
        MINUTE    | search(COMMENT, "added project")                     | 1
        MINUTE    | select(COMMENT, "added project")                     | 1
        MINUTE    | not(select(COMMENT, "added project"))                | 1393
        MINUTE    | or(select(IS_ROBOT, TRUE), select(IS_ROBOT, FALSE))  | 1394
        MINUTE    | and(select(IS_ROBOT, TRUE), select(IS_ROBOT, FALSE)) | 0 // todo why does this fail
        HOUR      | null                                                 | 24
        DAY       | null                                                 | 1
        WEEK      | null                                                 | 1
        MONTH     | null                                                 | 1
        YEAR      | null                                                 | 1

    }

    def "Test response parsing and looking like druid's results"() {
        // todo make tests
    }
}
