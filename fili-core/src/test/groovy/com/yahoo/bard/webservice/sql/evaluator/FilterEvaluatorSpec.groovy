// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator

import static com.yahoo.bard.webservice.sql.helper.Filters.and
import static com.yahoo.bard.webservice.sql.helper.Filters.not
import static com.yahoo.bard.webservice.sql.helper.Filters.or
import static com.yahoo.bard.webservice.sql.helper.Filters.search
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.ID
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.IS_NEW
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.IS_ROBOT
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.METRO_CODE
import static com.yahoo.bard.webservice.sql.helper.SimpleDruidQueryBuilder.WIKITICKER

import com.yahoo.bard.webservice.sql.helper.CalciteHelper
import com.yahoo.bard.webservice.sql.database.Database

import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.tools.RelBuilder

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Connection
import java.util.stream.Collectors

class FilterEvaluatorSpec extends Specification {
    static Connection CONNECTION = Database.initializeDatabase()
    static FilterEvaluator filterEvaluator = new FilterEvaluator()

    @Unroll
    def "GetDimensionNames expecting #dimensions"() {
        setup:
        RelBuilder builder = CalciteHelper.getBuilder(Database.getDataSource())
        FilterEvaluator filterEvaluator = new FilterEvaluator()
        builder.scan(WIKITICKER)
        def rexNodes = dimensions.stream()
                .map { builder.field(it) }
                .collect(Collectors.toList())

        expect:
        builder.project(rexNodes)
        List<String> foundDimensions = filterEvaluator.getDimensionNames(builder, filter)
        dimensions.stream().forEach { foundDimensions.contains(it) }
        foundDimensions.stream().forEach { dimensions.contains(it) }
        String sql = new RelToSqlConverter(SqlDialect.create(CONNECTION.getMetaData())).visitChild(0, builder.build()).
                asSelect().
                toString();
        sql == "SELECT " + dimensions.stream().
                map { "`" + it + "`" }.
                collect(Collectors.joining(", ")) + "\n" +
                "FROM `${CalciteHelper.DEFAULT_SCHEMA}`.`${WIKITICKER}`"

        where: "we have"
        filter                                                | dimensions
        search(ID)                                            | [ID] as List<String>
        or(search(ID), search(ID))                            | [ID] as List<String>
        not(not(not(search(ID))))                             | [ID] as List<String>
        and(search(ID), or(search(IS_NEW), search(IS_ROBOT))) | [ID, IS_NEW, IS_ROBOT] as List<String>
        and(
                search(ID),
                search(IS_NEW),
                or(search(IS_ROBOT), search(METRO_CODE))
        )                                                     | [ID, IS_NEW, IS_ROBOT, METRO_CODE] as
                List<String>
        not(
                and(
                        search(ID),
                        search(IS_NEW),
                        or(search(IS_ROBOT), search(METRO_CODE))
                )
        )                                                     | [ID, IS_NEW, IS_ROBOT, METRO_CODE] as List<String>
    }

    def "Test null input"() {
        setup:
        RelBuilder builder = CalciteHelper.getBuilder(Database.getDataSource())

        expect:
        filterEvaluator.evaluateFilter(builder, null) == null
    }
}
