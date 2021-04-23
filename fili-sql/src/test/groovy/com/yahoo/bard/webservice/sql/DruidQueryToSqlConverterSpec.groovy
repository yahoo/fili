// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql

import com.yahoo.bard.webservice.druid.model.aggregation.ThetaSketchAggregation
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.filter.NotFilter
import com.yahoo.bard.webservice.druid.model.filter.SearchFilter
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.druid.model.query.TopNQuery

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR
import static com.yahoo.bard.webservice.database.Database.ADDED
import static com.yahoo.bard.webservice.database.Database.DELETED
import static com.yahoo.bard.webservice.database.Database.DELTA
import static com.yahoo.bard.webservice.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.database.Database.METRO_CODE
import static com.yahoo.bard.webservice.database.Database.COUNT
import static com.yahoo.bard.webservice.database.Database.TIME
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.ASC
import static com.yahoo.bard.webservice.druid.model.orderby.SortDirection.DESC
import static com.yahoo.bard.webservice.data.time.AllGranularity.INSTANCE
import static com.yahoo.bard.webservice.sql.builders.Aggregator.count
import static com.yahoo.bard.webservice.sql.builders.Aggregator.sum
import static com.yahoo.bard.webservice.sql.builders.Aggregator.filteredLongSum
import static com.yahoo.bard.webservice.sql.builders.Intervals.interval
import static com.yahoo.bard.webservice.sql.builders.PostAggregator.fieldAccessorPostAggregation
import static com.yahoo.bard.webservice.sql.builders.PostAggregator.ratioPostAggregator
import static com.yahoo.bard.webservice.sql.builders.PostAggregator.sumPostAggregator
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.END
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.START
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getDimensions
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getDimension
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getTopNMetric
import static com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder.getWikitickerDatasource

import com.yahoo.bard.webservice.database.Database
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.query.DruidQuery
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.sql.builders.SimpleDruidQueryBuilder
import com.yahoo.bard.webservice.sql.helper.CalciteHelper

import spock.lang.Specification
import spock.lang.Unroll

class DruidQueryToSqlConverterSpec extends Specification {
    static CalciteHelper calciteHelper = new CalciteHelper(Database.getDataSource())
    static DruidQueryToSqlConverter druidQueryToSqlConverter =
            new DruidQueryToSqlConverter(calciteHelper, "yyyyMMddHH", "yyyyMMdd")
    static ApiToFieldMapper apiToFieldMapper = SimpleDruidQueryBuilder.getApiToFieldMapper(API_PREPEND, "")
    static public final String API_PREPEND = "api_"

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
                [sum(ADDED), sum(DELETED)],
                [],
                [interval(START, END)],
                limitSpec,
                null
        )
    }


    private static TopNQuery getTopNQuery(
            Granularity timeGrain,
            String dimension,
            long threshold,
            String topNMetric,
            SortDirection direction
    ){
        return new TopNQuery(
                getWikitickerDatasource(API_PREPEND, ""),
                timeGrain,
                getDimension(API_PREPEND + dimension),
                null,
                [sum(ADDED), sum(DELETED)],
                [],
                [interval(START, END)],
                threshold,
                getTopNMetric(API_PREPEND + topNMetric, direction),
                null
        );
    }

    private static GroupByQuery getGroupByQueryWithCount(
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
                [sum(ADDED), sum(DELETED), count()],
                [],
                [interval(START, END)],
                limitSpec,
                null
        )
    }

    private static GroupByQuery getGroupByQueryWithFilteredRatio(
            Granularity timeGrain,
            List<String> dimensions,
            Filter filter,
            LimitSpec limitSpec
    ) {
        return new GroupByQuery(
                getWikitickerDatasource(API_PREPEND, ""),
                timeGrain,
                getDimensions(dimensions.collect { API_PREPEND + it }),
                null,
                null,
                [sum(ADDED), sum(DELETED), filteredLongSum(ADDED + "Filtered", sum(ADDED), filter)],
                [
                        ratioPostAggregator(ADDED + "Ratio", [
                                fieldAccessorPostAggregation(sum(ADDED + "Filtered")),
                                sumPostAggregator("addedAndDeleted", [
                                        fieldAccessorPostAggregation(sum(ADDED)),
                                        fieldAccessorPostAggregation(sum(DELETED))
                                ])
                        ])
                ],
                [interval(START, END)],
                limitSpec,
                null
        )
    }

    private static GroupByQuery getGroupByQueryWithMultipleFilteredRatio(
            Granularity timeGrain,
            List<String> dimensions,
            LimitSpec limitSpec
    ) {
        Filter filter1 = new SearchFilter(getDimension(METRO_CODE), SearchFilter.QueryType.Contains, "search");
        Filter filter2 = new NotFilter(filter1);
        return new GroupByQuery(
                getWikitickerDatasource(API_PREPEND, ""),
                timeGrain,
                getDimensions(dimensions.collect { API_PREPEND + it }),
                null,
                null,
                [
                        sum(ADDED),
                        sum(DELETED),
                        sum(DELTA),
                        filteredLongSum(ADDED + "Filtered", sum(ADDED), filter1),
                        filteredLongSum(DELTA + "Filtered", sum(DELTA), filter2),
                ],
                [
                        ratioPostAggregator(ADDED + "Ratio", [
                                fieldAccessorPostAggregation(sum(ADDED + "Filtered")),
                                sumPostAggregator("addedAndDeleted", [
                                        fieldAccessorPostAggregation(sum(ADDED)),
                                        fieldAccessorPostAggregation(sum(DELETED))
                                ])
                        ]),
                        ratioPostAggregator(DELTA + "Ratio", [
                                fieldAccessorPostAggregation(sum(DELTA + "Filtered")),
                                sumPostAggregator("addedDeletedDelta", [
                                        fieldAccessorPostAggregation(sum(ADDED)),
                                        fieldAccessorPostAggregation(sum(DELETED)),
                                        fieldAccessorPostAggregation(sum(DELTA))
                                ])
                        ])
                ],
                [interval(START, END)],
                limitSpec,
                null
        )
    }

    private static GroupByQuery getGroupByQueryWithSketchMetric(
            Granularity timeGrain,
            List<String> dimensions,
            LimitSpec limitSpec
    ) {
        return new GroupByQuery(
                getWikitickerDatasource(API_PREPEND, ""),
                timeGrain,
                getDimensions(dimensions.collect {API_PREPEND + it }),
                null,
                null,
                [new ThetaSketchAggregation("foo", "bar", 128)],
                [],
                [interval(START, END)],
                limitSpec,
                null
        )
    }

    @Unroll
    def "test sorting on #dims with #metrics by #metricDirections"() {
        setup:
        DruidQuery query = getGroupByQuery(grain, dims, SimpleDruidQueryBuilder.getSort(metrics, metricDirections))
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql.endsWith(expectedOutput.trim())

        where:
        grain    | dims                   | metrics          | metricDirections | expectedOutput
        DAY      | [METRO_CODE]           | [ADDED]          | [DESC]           | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), SUM(CAST("${ADDED}" AS DOUBLE)) DESC NULLS FIRST, "${METRO_CODE}" """
        DAY      | [METRO_CODE, IS_ROBOT] | [ADDED]          | [DESC]           | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), SUM(CAST("${ADDED}" AS DOUBLE)) DESC NULLS FIRST, "${METRO_CODE}", "${IS_ROBOT}" """
        DAY      | []                     | [ADDED, DELETED] | [DESC, ASC]      | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), SUM(CAST("${ADDED}" AS DOUBLE)) DESC NULLS FIRST, SUM(CAST("${DELETED}" AS DOUBLE)) """
        YEAR     | [METRO_CODE]           | []               | []               | """ ORDER BY YEAR("${TIME}"), "${METRO_CODE}" """
        MONTH    | []                     | []               | []               | """ ORDER BY YEAR("${TIME}"), MONTH("${TIME}") """
        INSTANCE | []                     | []               | []               | """ WHERE "${TIME}" >= '20150912' AND "${TIME}" < '20150913' """
    }

    def "test COUNT(*) in SELECT"() {
        setup:
        DruidQuery query = getGroupByQueryWithCount(grain, dims, null)
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql.startsWith(expectedOutput.trim())

        where:
        grain | dims         | expectedOutput
        DAY   | [METRO_CODE] | """ SELECT "${METRO_CODE}", YEAR("${TIME}") AS "YEAR", DAYOFYEAR("${TIME}") AS "DAYOFYEAR", SUM(CAST("${ADDED}" AS DOUBLE)) AS "${API_PREPEND}${ADDED}", SUM(CAST("${DELETED}" AS DOUBLE)) AS "${API_PREPEND}${DELETED}", COUNT(*) AS "count" """
    }

    def "test COUNT(*) in ORDER BY"() {
        setup:
        DruidQuery query = getGroupByQueryWithCount(grain, dims, SimpleDruidQueryBuilder.getSort(metrics, metricDirections))
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql.endsWith(expectedOutput.trim())

        where:
        grain | dims         | metrics        | metricDirections | expectedOutput
        DAY   | [METRO_CODE] | [COUNT]        | [DESC]           | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), COUNT(*) DESC NULLS FIRST, "${METRO_CODE}" """
        DAY   | [METRO_CODE] | [ADDED, COUNT] | [DESC, DESC]     | """ ORDER BY YEAR("${TIME}"), DAYOFYEAR("${TIME}"), SUM(CAST("${ADDED}" AS DOUBLE)) DESC NULLS FIRST, COUNT(*) DESC NULLS FIRST, "${METRO_CODE}" """
    }

    @Unroll
    def "test single filtered addRatio with #filter"() {
        setup:
        DruidQuery query = getGroupByQueryWithFilteredRatio(
                grain,
                dims,
                filter,
                SimpleDruidQueryBuilder.getSort(metrics, metricDirections)
        )
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql == expectedOutput.trim()

        where:
        grain | dims         | filter                                                                                | metrics          | metricDirections | expectedOutput
        DAY   | [METRO_CODE] | new SearchFilter(getDimension(METRO_CODE), SearchFilter.QueryType.Contains, "search") | [ADDED, DELETED] | [DESC, DESC]     | """
SELECT "metroCode", "YEAR", "DAYOFYEAR", SUM("api_added") AS "api_added", SUM("api_deleted") AS "api_deleted", SUM("api_addedFiltered") AS "api_addedFiltered", 1.0 * SUM("api_addedFiltered") / (SUM("api_added") + SUM("api_deleted")) AS "addedRatio"
FROM (SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM("added") AS "api_added", SUM("deleted") AS "api_deleted", SUM(0) AS "api_addedFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")
            UNION ALL
            SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM(0) AS "api_added", SUM(0) AS "api_deleted", SUM("added") AS "api_addedFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913' AND "metroCode" LIKE '%search%'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")) AS "t6"
GROUP BY "metroCode", "YEAR", "DAYOFYEAR"
ORDER BY "YEAR", "DAYOFYEAR", SUM("api_added") DESC NULLS FIRST, SUM("api_deleted") DESC NULLS FIRST, "metroCode"
"""
        DAY   | [METRO_CODE] | new SelectorFilter(getDimension(METRO_CODE), "selector")                              | [ADDED, DELETED] | [DESC, DESC]     | """
SELECT "metroCode", "YEAR", "DAYOFYEAR", SUM("api_added") AS "api_added", SUM("api_deleted") AS "api_deleted", SUM("api_addedFiltered") AS "api_addedFiltered", 1.0 * SUM("api_addedFiltered") / (SUM("api_added") + SUM("api_deleted")) AS "addedRatio"
FROM (SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM("added") AS "api_added", SUM("deleted") AS "api_deleted", SUM(0) AS "api_addedFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")
            UNION ALL
            SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM(0) AS "api_added", SUM(0) AS "api_deleted", SUM("added") AS "api_addedFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913' AND "metroCode" = 'selector'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")) AS "t6"
GROUP BY "metroCode", "YEAR", "DAYOFYEAR"
ORDER BY "YEAR", "DAYOFYEAR", SUM("api_added") DESC NULLS FIRST, SUM("api_deleted") DESC NULLS FIRST, "metroCode"
"""
        DAY   | [METRO_CODE] | new NotFilter(new SelectorFilter(getDimension(METRO_CODE), "not"))                    | [ADDED, DELETED] | [DESC, DESC]     | """
SELECT "metroCode", "YEAR", "DAYOFYEAR", SUM("api_added") AS "api_added", SUM("api_deleted") AS "api_deleted", SUM("api_addedFiltered") AS "api_addedFiltered", 1.0 * SUM("api_addedFiltered") / (SUM("api_added") + SUM("api_deleted")) AS "addedRatio"
FROM (SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM("added") AS "api_added", SUM("deleted") AS "api_deleted", SUM(0) AS "api_addedFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")
            UNION ALL
            SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM(0) AS "api_added", SUM(0) AS "api_deleted", SUM("added") AS "api_addedFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913' AND ("metroCode" < 'not' OR "metroCode" > 'not')
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")) AS "t6"
GROUP BY "metroCode", "YEAR", "DAYOFYEAR"
ORDER BY "YEAR", "DAYOFYEAR", SUM("api_added") DESC NULLS FIRST, SUM("api_deleted") DESC NULLS FIRST, "metroCode"
"""
    }

    def "test multiple filtered addRatio"() {
        setup:
        DruidQuery query = getGroupByQueryWithMultipleFilteredRatio(
                grain,
                dims,
                SimpleDruidQueryBuilder.getSort(metrics, metricDirections)
        )
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql == expectedOutput.trim()

        where:
        grain | dims         | metrics          | metricDirections | expectedOutput
        DAY   | [METRO_CODE] | [ADDED, DELETED] | [DESC, DESC]     | """
SELECT "metroCode", "YEAR", "DAYOFYEAR", SUM("api_added") AS "api_added", SUM("api_deleted") AS "api_deleted", SUM("api_delta") AS "api_delta", SUM("api_addedFiltered") AS "api_addedFiltered", SUM("api_deltaFiltered") AS "api_deltaFiltered", 1.0 * SUM("api_addedFiltered") / (SUM("api_added") + SUM("api_deleted")) AS "addedRatio", 1.0 * SUM("api_deltaFiltered") / (SUM("api_added") + SUM("api_deleted") + SUM("api_delta")) AS "deltaRatio"
FROM (SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM("added") AS "api_added", SUM("deleted") AS "api_deleted", SUM("delta") AS "api_delta", SUM(0) AS "api_addedFiltered", SUM(0) AS "api_deltaFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")
            UNION ALL
            SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM(0) AS "api_added", SUM(0) AS "api_deleted", SUM(0) AS "api_delta", SUM("added") AS "api_addedFiltered", SUM(0) AS "api_deltaFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913' AND "metroCode" LIKE '%search%'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")
            UNION ALL
            SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM(0) AS "api_added", SUM(0) AS "api_deleted", SUM(0) AS "api_delta", SUM(0) AS "api_addedFiltered", SUM("delta") AS "api_deltaFiltered"
            FROM "PUBLIC"."wikiticker"
            WHERE "TIME" >= '20150912' AND "TIME" < '20150913' AND "metroCode" NOT LIKE '%search%'
            GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")) AS "t11"
GROUP BY "metroCode", "YEAR", "DAYOFYEAR"
ORDER BY "YEAR", "DAYOFYEAR", SUM("api_added") DESC NULLS FIRST, SUM("api_deleted") DESC NULLS FIRST, "metroCode"
"""
    }

    def "test topN"() {
        setup:
        DruidQuery query = getTopNQuery(
                grain,
                dim,
                threshold,
                topNMetric,
                direction
        )
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql.equals(expectedOutput.trim())

        where:
        grain | dim         | threshold | topNMetric   | direction  | expectedOutput
        HOUR  | METRO_CODE  | 3l        | ADDED        | DESC       | """
SELECT *
FROM (SELECT "metroCode", "YEAR", "DAYOFYEAR", "HOUR", "api_added", "api_deleted", ROW_NUMBER() OVER (PARTITION BY "YEAR", "DAYOFYEAR", "HOUR" ORDER BY "api_added" DESC) AS "RNUM"
        FROM (SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", HOUR("TIME") AS "HOUR", SUM(CAST("added" AS DOUBLE)) AS "api_added", SUM(CAST("deleted" AS DOUBLE)) AS "api_deleted"
                FROM "PUBLIC"."wikiticker"
                WHERE "TIME" >= '20150912' AND "TIME" < '20150913'
                GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME"), HOUR("TIME")
                ORDER BY YEAR("TIME"), DAYOFYEAR("TIME"), HOUR("TIME"), "metroCode") AS "t2") AS "t3"
WHERE "RNUM" <= 3
ORDER BY "YEAR" NULLS LAST, "DAYOFYEAR" NULLS LAST, "HOUR" NULLS LAST, "RNUM" NULLS LAST
"""
        DAY   | METRO_CODE  | 4l        | DELETED      | ASC        | """
SELECT *
FROM (SELECT "metroCode", "YEAR", "DAYOFYEAR", "api_added", "api_deleted", ROW_NUMBER() OVER (PARTITION BY "YEAR", "DAYOFYEAR" ORDER BY "api_deleted") AS "RNUM"
        FROM (SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", SUM(CAST("added" AS DOUBLE)) AS "api_added", SUM(CAST("deleted" AS DOUBLE)) AS "api_deleted"
                FROM "PUBLIC"."wikiticker"
                WHERE "TIME" >= '20150912' AND "TIME" < '20150913'
                GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME")
                ORDER BY YEAR("TIME"), DAYOFYEAR("TIME"), "metroCode") AS "t2") AS "t3"
WHERE "RNUM" <= 4
ORDER BY "YEAR" NULLS LAST, "DAYOFYEAR" NULLS LAST, "RNUM" NULLS LAST
"""
    }

    def "test sketch metrics"() {
        setup:
        DruidQuery query = getGroupByQueryWithSketchMetric(
                grain,
                dims,
                null
        )
        def sql = druidQueryToSqlConverter.buildSqlQuery(query, apiToFieldMapper)

        expect:
        sql.equals(expectedOutput.trim())

        where:
        grain | dims         | metrics        | metricDirections | expectedOutput
        DAY   | [METRO_CODE] | [COUNT]        | [DESC]           | """SELECT "metroCode", YEAR("TIME") AS "YEAR", DAYOFYEAR("TIME") AS "DAYOFYEAR", round(thetasketch_estimate(thetasketch_union(bar))) AS "foo" FROM "PUBLIC"."wikiticker" WHERE "TIME" >= '20150912' AND "TIME" < '20150913' GROUP BY "metroCode", YEAR("TIME"), DAYOFYEAR("TIME") ORDER BY YEAR("TIME"), DAYOFYEAR("TIME"), "metroCode"
"""
    }
}
