// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice

import static com.yahoo.bard.webservice.helper.Filters.and
import static com.yahoo.bard.webservice.helper.Filters.not
import static com.yahoo.bard.webservice.helper.Filters.or
import static com.yahoo.bard.webservice.helper.Filters.search
import static com.yahoo.bard.webservice.helper.Filters.select
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MINUTE
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import com.yahoo.bard.webservice.data.DruidResponseParser
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.ResultSetSchema
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.DefaultQueryType
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.test.Database

import com.fasterxml.jackson.databind.JsonNode

import org.joda.time.DateTimeZone

import spock.lang.Specification
import spock.lang.Unroll
import com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder

import java.util.stream.Stream

/**
 * Created by hinterlong on 6/7/17.
 */
class SQLConverterSpec extends Specification {
    private static final SqlBackedClient sqlBackedClient = new SQLConverter(
            Database.getDatabase(),
            Database.getDataSource()
    )
    private static final String TABLE_NAME = "WIKITICKER"
    private static final String IS_ROBOT = "IS_ROBOT"
    private static final String COMMENT = "COMMENT"

    @Unroll
    def "ExecuteQuery for #timegrain want #size filter on #filter"() {
        expect:
        DruidQuery druidQuery = SimpleDruidQueryBuilder.timeSeriesQuery(
                TABLE_NAME,
                filter,
                timegrain as DefaultTimeGrain
        );
        JsonNode jsonNode = sqlBackedClient.executeQuery(druidQuery).get();

        DruidResponseParser druidResponseParser = new DruidResponseParser();

        List<Column> columns = new ArrayList<>();
        Stream.of("ADDED", "DELETED", "DELTA")
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
        timegrain | filter                                                   | size
        MINUTE    | null                                                     | 1394
        MINUTE    | search(COMMENT, "added project")                         | 1
        MINUTE    | select(COMMENT, "added project")                         | 1
        MINUTE    | not(select(COMMENT, "added project"))                    | 1393
        MINUTE    | or(select(IS_ROBOT, "TRUE"), select(IS_ROBOT, "FALSE"))  | 1394
        MINUTE    | and(select(IS_ROBOT, "TRUE"), select(IS_ROBOT, "FALSE")) | 0 // why does this fail
        HOUR      | null                                                     | 24
        DAY       | null                                                     | 1
        WEEK      | null                                                     | 1
        MONTH     | null                                                     | 1
        YEAR      | null                                                     | 1

    }
}
