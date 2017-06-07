package com.yahoo.bard.webservice

import static com.yahoo.bard.webservice.helper.Filters.and
import static com.yahoo.bard.webservice.helper.Filters.not
import static com.yahoo.bard.webservice.helper.Filters.or
import static com.yahoo.bard.webservice.helper.Filters.search

import com.yahoo.bard.webservice.test.Database

import org.apache.calcite.rel.rel2sql.RelToSqlConverter
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.tools.RelBuilder

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Connection
import java.util.stream.Collectors

/**
 * Created by hinterlong on 6/7/17.
 */
class FilterEvaluatorSpec extends Specification {
    private static final String DIM1 = "ID";
    private static final String DIM2 = "IS_NEW";
    private static final String DIM3 = "IS_ROBOT";
    private static final String DIM4 = "METRO_CODE"
    private final Connection CONNECTION = Database.getDatabase()
    private static final String TABLE_NAME = "WIKITICKER"

    @Unroll
    def "GetDimensionNames"() {
        setup:
        RelBuilder builder = SQLConverter.builder(CONNECTION, Database.getDataSource())
        builder.scan(TABLE_NAME)
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
                "FROM `DEFAULT_SCHEMA`.`${TABLE_NAME}`"

        where: "we have"
        filter                                            | dimensions
        search(DIM1)                                      | [DIM1] as List<String>
        or(search(DIM1), search(DIM1))                    | [DIM1] as List<String>
        not(not(not(search(DIM1))))                       | [DIM1] as List<String>
        and(search(DIM1), or(search(DIM2), search(DIM3))) | [DIM1, DIM2, DIM3] as List<String>
        and(
                search(DIM1),
                search(DIM2),
                or(search(DIM3), search(DIM4))
        )                                                 | [DIM1, DIM2, DIM3, DIM4] as
                List<String>
        not(
                and(
                        search(DIM1),
                        search(DIM2),
                        or(search(DIM3), search(DIM4))
                )
        )                                                 | [DIM1, DIM2, DIM3, DIM4] as List<String>
    }

    def "AddFilter"() {
        //todo maybe refactor this one
    }
}
