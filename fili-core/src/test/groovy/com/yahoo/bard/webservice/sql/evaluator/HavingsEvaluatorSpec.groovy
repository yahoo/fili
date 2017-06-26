// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator

import static com.yahoo.bard.webservice.sql.database.Database.ADDED
import static com.yahoo.bard.webservice.sql.database.Database.DELETED
import static com.yahoo.bard.webservice.sql.database.Database.WIKITICKER
import static com.yahoo.bard.webservice.sql.builders.Aggregator.max
import static com.yahoo.bard.webservice.sql.builders.Aggregator.min
import static com.yahoo.bard.webservice.sql.builders.Aggregator.sum
import static com.yahoo.bard.webservice.sql.builders.Havings.and
import static com.yahoo.bard.webservice.sql.builders.Havings.equals
import static com.yahoo.bard.webservice.sql.builders.Havings.gt
import static com.yahoo.bard.webservice.sql.builders.Havings.lt
import static com.yahoo.bard.webservice.sql.builders.Havings.or
import static java.util.Arrays.asList

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.sql.AliasMaker
import com.yahoo.bard.webservice.sql.database.Database
import com.yahoo.bard.webservice.sql.helper.CalciteHelper
import com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.sql.helper.SqlAggregationType
import com.yahoo.bard.webservice.sql.helper.TimeConverter

import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.tools.RelBuilder

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Connection
import java.util.stream.Collectors

class HavingsEvaluatorSpec extends Specification {
    private static final int ONE = 1
    private static final int TWO = 2
    private static final Connection CONNECTION = Database.initializeDatabase()
    private static final AliasMaker ALIAS_MAKER = new AliasMaker(SimpleDruidQueryBuilder.getDictionary().get(WIKITICKER).schema);

    private static RelBuilder getBuilder() {
        RelBuilder builder = CalciteHelper.getBuilder(Database.getDataSource())
        builder.scan(WIKITICKER);
        return builder
    }

    @Unroll
    def "GetDimensionNames on #expectedHavingSql"() {
        setup:
        RelBuilder builder = getBuilder()
        RelBuilder.AggCall[] aggregationCalls = aggregations.stream().map {
            return SqlAggregationType.getAggregation(it, builder)
        }.collect(Collectors.toList()).toArray() as RelBuilder.AggCall[]
        builder.aggregate(
                builder.groupKey(
                        TimeConverter.buildGroupBy(builder, DefaultTimeGrain.DAY, "TIME").collect(Collectors.toList())
                ),
                aggregationCalls
        )
        RexNode havingFilter = HavingEvaluator.buildFilter(builder, having, ALIAS_MAKER).get()
        builder.filter(havingFilter)

        expect:
        String sql = new RelToSqlConverter(SqlDialect.create(CONNECTION.getMetaData())).visitChild(0, builder.build()).
                asSelect().
                toString();
        sql.contains(expectedHavingSql)
        where: "we have"
        having                                    | aggregations                     | expectedHavingSql
        gt(ADDED, ONE)                            | asList(sum(ADDED))               | "HAVING SUM(`${ADDED}`) > 1"
        lt(DELETED, ONE)                          | asList(sum(DELETED))             | "HAVING SUM(`${DELETED}`) < 1"
        equals(DELETED, ONE)                      | asList(sum(DELETED), sum(ADDED)) | "HAVING SUM(`${DELETED}`) = 1"
        or(equals(DELETED, ONE), lt(ADDED, TWO))  | asList(max(DELETED), min(ADDED)) | "HAVING MAX(`${DELETED}`) = 1 OR MIN(`${ADDED}`) < 2"
        and(equals(DELETED, ONE), lt(ADDED, TWO)) | asList(max(DELETED), min(ADDED)) | "HAVING MAX(`${DELETED}`) = 1 AND MIN(`${ADDED}`) < 2"

    }

    @Unroll
    def "Test bad inputs for having filters on #expectedHavingSql"() {
        setup:
        RelBuilder builder = getBuilder()
        RelBuilder.AggCall[] aggregationCalls = aggregations.collect { SqlAggregationType.getAggregation(it, builder) } as RelBuilder.AggCall[]
        builder.aggregate(
                builder.groupKey(
                        TimeConverter.buildGroupBy(builder, DefaultTimeGrain.DAY, "TIME").collect(Collectors.toList())
                ),
                aggregationCalls
        )
        RexNode havingFilter = HavingEvaluator.buildFilter(builder, having, ALIAS_MAKER).get()
        builder.filter(havingFilter)

        expect:
        String sql = new RelToSqlConverter(SqlDialect.create(CONNECTION.getMetaData())).visitChild(0, builder.build()).
                asSelect().
                toString();
        sql.contains(expectedHavingSql)

        where: "queries have 2 or more aggregations on one metric - can't tell which should be used in having filter"
        having                                 | aggregations                       | expectedHavingSql
        or(lt(DELETED, ONE), gt(DELETED, TWO)) | asList(max(DELETED), min(DELETED)) | "HAVING MAX(`${DELETED}`) < 1 OR MAX(`${DELETED}`) > 2"
    }
}
