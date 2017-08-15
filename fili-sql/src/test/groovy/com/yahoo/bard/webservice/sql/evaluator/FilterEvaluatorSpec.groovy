// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.evaluator

import static com.yahoo.bard.webservice.database.Database.SCHEMA
import static com.yahoo.bard.webservice.sql.builders.Filters.and
import static com.yahoo.bard.webservice.sql.builders.Filters.not
import static com.yahoo.bard.webservice.sql.builders.Filters.or
import static com.yahoo.bard.webservice.sql.builders.Filters.search
import static com.yahoo.bard.webservice.database.Database.IS_NEW
import static com.yahoo.bard.webservice.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.database.Database.METRO_CODE
import static com.yahoo.bard.webservice.database.Database.WIKITICKER

import com.yahoo.bard.webservice.sql.ApiToFieldMapper
import com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.database.Database
import com.yahoo.bard.webservice.sql.helper.CalciteHelper

import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.tools.RelBuilder

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Connection

class FilterEvaluatorSpec extends Specification {
    static Connection CONNECTION = Database.initializeDatabase()
    static FilterEvaluator filterEvaluator = new FilterEvaluator()
    static final String API = "api_"

    @Unroll
    def "GetDimensionNames expecting #filterString"() {
        setup:
        ApiToFieldMapper apiToFieldMapper = SimpleDruidQueryBuilder.getApiToFieldMapper(API, "")
        RelBuilder builder = CalciteHelper.getBuilder(Database.getDataSource(), SCHEMA)
        builder.scan(WIKITICKER)
        def rexnode = filterEvaluator.evaluateFilter(builder, filter, apiToFieldMapper)
        builder.filter(rexnode)

        expect:
        String sql = new RelToSqlConverter(SqlDialect.create(CONNECTION.getMetaData())).visitChild(0, builder.build()).
                asSelect().
                toString()
        sql.endsWith(filterString)

        where: "we have"
        filter                                                                        | filterString
        search(API + IS_ROBOT)                                                        | "WHERE `isRobot` LIKE '%%'"
        or(search(API + IS_ROBOT), search(API + IS_ROBOT))                            | "WHERE `isRobot` LIKE '%%'"
        not(not(not(search(API + IS_ROBOT))))                                         | "WHERE NOT `isRobot` LIKE '%%'"
        and(search(API + IS_ROBOT), or(search(API + IS_NEW), search(API + IS_ROBOT))) | "WHERE `isRobot` LIKE '%%' AND (`isNew` LIKE '%%' OR `isRobot` LIKE '%%')"
        and(
                search(API + IS_ROBOT),
                search(API + IS_NEW, "the"),
                or(search(API + IS_ROBOT), search(API + METRO_CODE))
        )                                                                             | "WHERE `isRobot` LIKE '%%' AND `isNew` LIKE '%the%' AND (`isRobot` LIKE '%%' OR `metroCode` LIKE '%%')"
        not(
                and(
                        search(API + IS_ROBOT),
                        search(API + IS_NEW),
                        or(search(API + IS_ROBOT), search(API + METRO_CODE))
                )
        )                                                                             | "WHERE NOT `isRobot` LIKE '%%' OR NOT `isNew` LIKE '%%' OR NOT `isRobot` LIKE '%%' AND NOT `metroCode` LIKE '%%'"
    }

    def "Test null input"() {
        setup:
        RelBuilder builder = CalciteHelper.getBuilder(Database.getDataSource(), SCHEMA)

        expect:
        filterEvaluator.evaluateFilter(builder, null, null) == null
    }
}
