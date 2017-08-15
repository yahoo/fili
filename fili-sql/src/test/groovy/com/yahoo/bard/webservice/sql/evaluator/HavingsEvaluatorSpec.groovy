// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator

import static com.yahoo.bard.webservice.database.Database.SCHEMA
import static com.yahoo.bard.webservice.sql.builders.Aggregator.max
import static com.yahoo.bard.webservice.sql.builders.Aggregator.min
import static com.yahoo.bard.webservice.sql.builders.Aggregator.sum
import static com.yahoo.bard.webservice.sql.builders.Havings.and
import static com.yahoo.bard.webservice.sql.builders.Havings.equals
import static com.yahoo.bard.webservice.sql.builders.Havings.gt
import static com.yahoo.bard.webservice.sql.builders.Havings.lt
import static com.yahoo.bard.webservice.sql.builders.Havings.or
import static com.yahoo.bard.webservice.database.Database.ADDED
import static com.yahoo.bard.webservice.database.Database.DELETED
import static com.yahoo.bard.webservice.database.Database.WIKITICKER
import static java.util.Arrays.asList

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.sql.ApiToFieldMapper
import com.yahoo.bard.webservice.sql.aggregation.DefaultDruidSqlAggregationConverter
import com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.database.Database
import com.yahoo.bard.webservice.sql.helper.CalciteHelper
import com.yahoo.bard.webservice.sql.helper.DefaultSqlTimeConverter

import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.tools.RelBuilder

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Connection
import java.util.stream.Collectors

class HavingsEvaluatorSpec extends Specification {
    static int ONE = 1
    static int TWO = 2
    static Connection CONNECTION = Database.initializeDatabase()
    static DefaultSqlTimeConverter sqlTimeConverter = new DefaultSqlTimeConverter()
    static HavingEvaluator havingEvaluator = new HavingEvaluator()
    static DefaultDruidSqlAggregationConverter druidSqlTypeConverter = new DefaultDruidSqlAggregationConverter()
    static final String API = "api_"

    private static RelBuilder getBuilder() {
        RelBuilder builder = CalciteHelper.getBuilder(Database.getDataSource(), SCHEMA)
        builder.scan(WIKITICKER);
        return builder
    }

    @Unroll
    def "GetDimensionNames on #expectedHavingSql"() {
        setup:
        ApiToFieldMapper apiToFieldMapper = SimpleDruidQueryBuilder.getApiToFieldMapper(API, "")
        RelBuilder builder = getBuilder()
        RelBuilder.AggCall[] aggregationCalls = aggregations.stream().map {
            def sqlAggregation = druidSqlTypeConverter.apply(it, apiToFieldMapper).get()
            return builder.aggregateCall(
                    sqlAggregation.sqlAggFunction,
                    false,
                    null,
                    sqlAggregation.sqlAggregationAsName,
                    builder.field(sqlAggregation.sqlAggregationFieldName)
            )
        }.collect(Collectors.toList()).toArray() as RelBuilder.AggCall[]
        builder.aggregate(
                builder.groupKey(
                        sqlTimeConverter.buildGroupBy(builder, DefaultTimeGrain.DAY, "TIME")
                ),
                aggregationCalls
        )
        RexNode havingFilter = havingEvaluator.evaluateHaving(builder, having, apiToFieldMapper)
        builder.filter(havingFilter)

        expect:
        String sql = new RelToSqlConverter(SqlDialect.create(CONNECTION.getMetaData())).visitChild(0, builder.build()).
                asSelect().
                toString();
        sql.contains(expectedHavingSql)
        where: "we have"
        having                                                | aggregations                                 | expectedHavingSql
        gt(API + ADDED, ONE)                                  | asList(sum(API + ADDED))                     | "HAVING SUM(`${ADDED}`) > 1"
        lt(API + DELETED, ONE)                                | asList(sum(API + DELETED))                   | "HAVING SUM(`${DELETED}`) < 1"
        equals(API + DELETED, ONE)                            | asList(sum(API + DELETED), sum(API + ADDED)) | "HAVING SUM(`${DELETED}`) = 1"
        or(equals(API + DELETED, ONE), lt(API + ADDED, TWO))  | asList(max(API + DELETED), min(API + ADDED)) | "HAVING MAX(`${DELETED}`) = 1 OR MIN(`${ADDED}`) < 2"
        and(equals(API + DELETED, ONE), lt(API + ADDED, TWO)) | asList(max(API + DELETED), min(API + ADDED)) | "HAVING MAX(`${DELETED}`) = 1 AND MIN(`${ADDED}`) < 2"

    }

    def "Test null input"() {
        setup:
        RelBuilder builder = getBuilder()

        expect:
        havingEvaluator.evaluateHaving(builder, null, null) == null
    }
}
