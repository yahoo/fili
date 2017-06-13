package com.yahoo.bard.webservice.sql

import static com.yahoo.bard.webservice.helper.Filters.and
import static com.yahoo.bard.webservice.helper.Filters.not
import static com.yahoo.bard.webservice.helper.Filters.or
import static com.yahoo.bard.webservice.helper.Filters.search
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.ID
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.IS_NEW
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.IS_ROBOT
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.METRO_CODE
import static com.yahoo.bard.webservice.helper.SimpleDruidQueryBuilder.WIKITICKER

import com.yahoo.bard.webservice.sql.FilterEvaluator
import com.yahoo.bard.webservice.sql.SqlConverter
import com.yahoo.bard.webservice.test.Database

import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.tools.RelBuilder

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Connection
import java.util.stream.Collectors

class FilterEvaluatorSpec extends Specification {
    private final Connection CONNECTION = Database.getDatabase()

    @Unroll
    def "GetDimensionNames"() {
        setup:
        RelBuilder builder = SqlConverter.builder(CONNECTION, Database.getDataSource())
        builder.scan(WIKITICKER)
        def rexNodes = dimensions.stream()
                .map { builder.field(it) }
                .collect(Collectors.toList())

        expect:
        builder.project(rexNodes)
        List<String> foundDimensions = FilterEvaluator.getDimensionNames(builder, filter)
        dimensions.stream().forEach { foundDimensions.contains(it) }
        foundDimensions.stream().forEach { dimensions.contains(it) }
        String sql = new RelToSqlConverter(SqlDialect.create(CONNECTION.getMetaData())).visitChild(0, builder.build()).
                asSelect().
                toString();
        sql == "SELECT " + dimensions.stream().
                map { "`" + it + "`" }.
                collect(Collectors.joining(", ")) + "\n" +
                "FROM `DEFAULT_SCHEMA`.`${WIKITICKER}`"

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
}
