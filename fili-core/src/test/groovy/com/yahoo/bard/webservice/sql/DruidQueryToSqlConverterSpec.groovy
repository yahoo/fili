package com.yahoo.bard.webservice.sql

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.ASC
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.DESC
import static com.yahoo.bard.webservice.druid.model.query.AllGranularity.INSTANCE
import static com.yahoo.bard.webservice.sql.builders.Aggregator.sum
import static com.yahoo.bard.webservice.sql.builders.Intervals.interval
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.END
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.START
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getDimensions
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getWikitickerDatasource
import static com.yahoo.bard.webservice.sql.database.Database.ADDED
import static com.yahoo.bard.webservice.sql.database.Database.DELETED
import static com.yahoo.bard.webservice.sql.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.sql.database.Database.METRO_CODE
import static java.util.Arrays.asList

import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.sql.database.Database
import com.yahoo.bard.webservice.sql.helper.CalciteHelper

import spock.lang.Specification
import spock.lang.Unroll

class DruidQueryToSqlConverterSpec extends Specification {
    static CalciteHelper calciteHelper = new CalciteHelper(Database.getDataSource(), CalciteHelper.DEFAULT_SCHEMA)
    static DruidQueryToSqlConverter druidQueryToSqlConverter = new DruidQueryToSqlConverter(calciteHelper)
    static ApiToFieldMapper apiToFieldMapper = SimpleDruidQueryBuilder.getApiToFieldMapper("api_", "")

    private static GroupByQuery getGroupByQuery(
            Granularity timeGrain,
            List<String> dimensions,
            LimitSpec limitSpec
    ) {
        return new GroupByQuery(
                getWikitickerDatasource("api_", ""),
                timeGrain,
                getDimensions(dimensions.collect { "api_" + it }),
                null,
                null,
                asList(sum("api_" + ADDED), sum("api_" + DELETED)),
                asList(),
                asList(interval(START, END)),
                limitSpec
        )
    }

    private static LimitSpec getSort(List<String> columns, List<SortDirection> sortDirections) {
        LinkedHashSet<OrderByColumn> sorts = new LinkedHashSet<>()
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
        def sql = druidQueryToSqlConverter.buildSqlQuery(calciteHelper.getConnection(), query, apiToFieldMapper)

        expect:
        sql.endsWith(expectedOutput)

        where:
        grain    | dims                         | metrics                | metricDirections  | expectedOutput
        DAY      | asList(METRO_CODE)           | asList(ADDED)          | asList(DESC)      | 'ORDER BY YEAR("TIME"), DAYOFYEAR("TIME"), SUM("added") DESC NULLS FIRST, "metroCode"'
        DAY      | asList(METRO_CODE, IS_ROBOT) | asList(ADDED)          | asList(DESC)      | 'ORDER BY YEAR("TIME"), DAYOFYEAR("TIME"), SUM("added") DESC NULLS FIRST, "metroCode", "isRobot"'
        DAY      | asList()                     | asList(ADDED, DELETED) | asList(DESC, ASC) | 'ORDER BY YEAR("TIME"), DAYOFYEAR("TIME"), SUM("added") DESC NULLS FIRST, SUM("deleted")'
        YEAR     | asList(METRO_CODE)           | asList()               | asList()          | 'ORDER BY YEAR("TIME"), "metroCode"'
        MONTH    | asList()                     | asList()               | asList()          | 'ORDER BY YEAR("TIME"), MONTH("TIME")'
        INSTANCE | asList()                     | asList()               | asList()          | 'ORDER BY "TIME"'
    }
}
