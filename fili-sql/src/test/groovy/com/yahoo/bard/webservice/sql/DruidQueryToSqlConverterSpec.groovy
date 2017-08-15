package com.yahoo.bard.webservice.sql

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.database.Database.ADDED
import static com.yahoo.bard.webservice.database.Database.DELETED
import static com.yahoo.bard.webservice.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.database.Database.METRO_CODE
import static com.yahoo.bard.webservice.database.Database.TIME
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.ASC
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.DESC
import static com.yahoo.bard.webservice.druid.model.query.AllGranularity.INSTANCE
import static com.yahoo.bard.webservice.sql.builders.Aggregator.sum
import static com.yahoo.bard.webservice.sql.builders.Intervals.interval
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.END
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.START
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getDimensions
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getWikitickerDatasource

import com.yahoo.bard.webservice.database.Database
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.sql.helper.CalciteHelper

import spock.lang.Specification
import spock.lang.Unroll

class DruidQueryToSqlConverterSpec extends Specification {
    static CalciteHelper calciteHelper = new CalciteHelper(Database.getDataSource())
    static DruidQueryToSqlConverter druidQueryToSqlConverter = new DruidQueryToSqlConverter(calciteHelper)
    static ApiToFieldMapper apiToFieldMapper = SimpleDruidQueryBuilder.getApiToFieldMapper(API_PREPEND, "")
    static private final String API_PREPEND = "api_"

    private static GroupByQuery getGroupByQuery(
            Granularity timeGrain,
            List<String> dimensions,
            LimitSpec limitSpec
    ) {
        return new GroupByQuery(
                getWikitickerDatasource(API_PREPEND, ""),
                timeGrain,
                getDimensions(dimensions.collect { API_PREPEND + it }),
                null,
                null,
                [sum(API_PREPEND + ADDED), sum(API_PREPEND + DELETED)],
                [],
                [interval(START, END)],
                limitSpec
        )
    }

    private static LimitSpec getSort(List<String> columns, List<SortDirection> sortDirections) {
        LinkedHashSet<OrderByColumn> sorts = []
        for (int i = 0; i < columns.size(); i++) {
            sorts.add(
                    new OrderByColumn(columns.get(i), sortDirections.get(i))
            )
        }

        return new LimitSpec(sorts)
    }

    @Unroll
    def "test sorting on #dims with #metrics by #metricDirections"() {
        setup:
        DruidQuery query = getGroupByQuery(grain, dims, getSort(metrics, metricDirections))
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql.endsWith(expectedOutput.trim())

        where:
        grain    | dims                   | metrics          | metricDirections | expectedOutput
        DAY      | [METRO_CODE]           | [ADDED]          | [DESC]           | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), SUM("${ADDED}") DESC NULLS FIRST, "${METRO_CODE}" """
        DAY      | [METRO_CODE, IS_ROBOT] | [ADDED]          | [DESC]           | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), SUM("${ADDED}") DESC NULLS FIRST, "${METRO_CODE}", "${IS_ROBOT}" """
        DAY      | []                     | [ADDED, DELETED] | [DESC, ASC]      | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), SUM("${ADDED}") DESC NULLS FIRST, SUM("${DELETED}") """
        YEAR     | [METRO_CODE]           | []               | []               | """ ORDER BY YEAR("${TIME}"), "${METRO_CODE}" """
        MONTH    | []                     | []               | []               | """ ORDER BY YEAR("${TIME}"), MONTH("${TIME}") """
        INSTANCE | []                     | []               | []               | """ ORDER BY "${TIME}" """
    }
}
