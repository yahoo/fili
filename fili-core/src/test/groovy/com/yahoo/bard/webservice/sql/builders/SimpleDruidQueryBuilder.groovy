// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import static com.yahoo.bard.webservice.sql.database.Database.ADDED
import static com.yahoo.bard.webservice.sql.database.Database.CHANNEL
import static com.yahoo.bard.webservice.sql.database.Database.CITY_NAME
import static com.yahoo.bard.webservice.sql.database.Database.COMMENT
import static com.yahoo.bard.webservice.sql.database.Database.COUNTRY_ISO_CODE
import static com.yahoo.bard.webservice.sql.database.Database.COUNTRY_NAME
import static com.yahoo.bard.webservice.sql.database.Database.DELETED
import static com.yahoo.bard.webservice.sql.database.Database.DELTA
import static com.yahoo.bard.webservice.sql.database.Database.IS_ANONYMOUS
import static com.yahoo.bard.webservice.sql.database.Database.IS_MINOR
import static com.yahoo.bard.webservice.sql.database.Database.IS_NEW
import static com.yahoo.bard.webservice.sql.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.sql.database.Database.IS_UNPATROLLED
import static com.yahoo.bard.webservice.sql.database.Database.METRO_CODE
import static com.yahoo.bard.webservice.sql.database.Database.NAMESPACE
import static com.yahoo.bard.webservice.sql.database.Database.PAGE
import static com.yahoo.bard.webservice.sql.database.Database.REGION_ISO_CODE
import static com.yahoo.bard.webservice.sql.database.Database.REGION_NAME
import static com.yahoo.bard.webservice.sql.database.Database.USER
import static com.yahoo.bard.webservice.sql.database.Database.WIKITICKER
import static java.util.Arrays.asList

import com.yahoo.bard.webservice.data.config.dimension.DefaultDimensionField
import com.yahoo.bard.webservice.data.config.dimension.DefaultKeyValueStoreDimensionConfig
import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.MapStoreManager
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.metadata.DataSourceMetadata
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.sql.ApiToFieldMapper
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.StrictPhysicalTable
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.Utils

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import java.util.stream.Collectors

class SimpleDruidQueryBuilder {
    public static final String START = "2015-09-12T00:00:00.000Z"
    public static final String END = "2015-09-13T00:00:00.000Z"

    public static ApiToFieldMapper getApiToFieldMapper() {
        return new ApiToFieldMapper(getDictionary().get(WIKITICKER).schema)
    }

    public static ApiToFieldMapper getApiToFieldMapper(String apiPrepend, String fieldPrepend) {
        return new ApiToFieldMapper(getDictionary(apiPrepend, fieldPrepend).get(WIKITICKER).schema)
    }

    public static PhysicalTableDictionary getDictionary() {
        return getDictionary("", "")
    }

    public static PhysicalTableDictionary getDictionary(String apiPrepend, String fieldPrepend) {
        def dataSource = getWikitickerDatasource(apiPrepend, fieldPrepend)
        PhysicalTableDictionary physicalTableDictionary = new PhysicalTableDictionary()
        physicalTableDictionary.put(WIKITICKER, dataSource.getPhysicalTable().sourceTable as ConfigPhysicalTable)
        return physicalTableDictionary
    }

    public static TableDataSource getWikitickerDatasource(String apiPrepend, String fieldPrepend) {
        return dataSource(
                WIKITICKER,
                DefaultTimeGrain.DAY,
                DateTimeZone.UTC,
                asList(ADDED, DELETED, DELTA),
                asList(
                        COUNTRY_ISO_CODE, IS_NEW, IS_ROBOT, PAGE,
                        USER, COMMENT, IS_UNPATROLLED, NAMESPACE,
                        COUNTRY_NAME, CITY_NAME, IS_MINOR, IS_ANONYMOUS,
                        REGION_ISO_CODE, CHANNEL, REGION_NAME, METRO_CODE
                ),
                apiPrepend,
                fieldPrepend
        )
    }

    public static TimeSeriesQuery timeSeriesQuery(
            String name,
            Filter filter,
            Granularity timeGrain,
            List<String> metrics,
            List<String> dimensions,
            List<Aggregation> aggregations,
            List<PostAggregation> postAggs,
            List<Interval> intervals
    ) {
        return new TimeSeriesQuery(
                dataSource(name, metrics, dimensions),
                timeGrain,
                filter,
                aggregations,
                postAggs,
                intervals
        );
    }

    public static GroupByQuery groupByQuery(
            String name,
            Filter filter,
            Having having,
            List<Dimension> groupByDimensions,
            Granularity timeGrain,
            List<String> metrics,
            List<String> dimensions,
            List<Aggregation> aggregations,
            List<PostAggregation> postAggs,
            List<Interval> intervals,
            LimitSpec limitSpec
    ) {
        return new GroupByQuery(
                dataSource(name, metrics, dimensions),
                timeGrain,
                groupByDimensions,
                filter,
                having,
                aggregations,
                postAggs,
                intervals,
                limitSpec
        )
    }

    public static TableDataSource dataSource(String name, List<String> metrics, List<String> dimensions) {
        return dataSource(name, DefaultTimeGrain.DAY, DateTimeZone.UTC, metrics, dimensions, "", "")
    }


    public static TableDataSource dataSource(
            String name,
            ZonelessTimeGrain zonelessTimeGrain,
            DateTimeZone dateTimeZone,
            List<String> metrics,
            List<String> dimensions,
            String apiPrepend,
            String fieldPrepend
    ) {
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(zonelessTimeGrain, dateTimeZone);
        Set<Column> columns = setOf();
        Map<String, String> logicalToPhysicalColumnNames = new HashMap<>()
        metrics.forEach { logicalToPhysicalColumnNames.put(apiPrepend + it, fieldPrepend + it) }
        dimensions.forEach { logicalToPhysicalColumnNames.put(apiPrepend + it, fieldPrepend + it) }

        DataSourceMetadataService metadataService = new DataSourceMetadataService();
        metadataService.update(
                DataSourceName.of(name),
                new DataSourceMetadata(name, Collections.emptyMap(), Collections.emptyList())
        );

        dimensions = dimensions.collect { apiPrepend + it }
        metrics = metrics.collect { apiPrepend + it }

        Set<String> metricsAndDimensions = new HashSet<>()
        metrics.forEach{ metricsAndDimensions.add(apiPrepend + it) }
        dimensions.forEach{ metricsAndDimensions.add(apiPrepend + it) }
        def strictPhysicalTable = new StrictPhysicalTable(
                TableName.of(name),
                zonedTimeGrain,
                columns,
                logicalToPhysicalColumnNames,
                metadataService
        )

        return new TableDataSource(
                new ConstrainedTable(
                        strictPhysicalTable,
                        new DataSourceConstraint(
                                setOf(),
                                setOf(),
                                setOf(),
                                setOf(metrics),
                                setOf(getDimensions(dimensions)),
                                setOf(dimensions),
                                metricsAndDimensions,
                                Collections.emptyMap()
                        )
                )
        );
    }

    public static List<Dimension> getDimensions(String... dimensions) {
        return getDimensions(asList(dimensions))
    }

    public static List<Dimension> getDimensions(List<String> dimensions) {
        return dimensions.stream()
                .map { getDimension(it as String) }
                .collect(Collectors.toList());
    }

    public static Dimension getDimension(String dimension) {
        return new KeyValueStoreDimension(
                new DefaultKeyValueStoreDimensionConfig(
                        { dimension },
                        dimension,
                        "",
                        "longName_" + dimension,
                        "General",
                        Utils.asLinkedHashSet(DefaultDimensionField.ID),
                        MapStoreManager.getInstance(dimension),
                        ScanSearchProviderManager.getInstance(dimension)
                )
        )
    }

    public static <T> Set<T> setOf(T... e) {
        return e == null ? Collections.emptySet() : new HashSet<>(asList(e));
    }

    public static <T> Set<T> setOf(List<T> e) {
        return new HashSet<T>(e);
    }

    public static ArithmeticPostAggregation arithmeticPA() {
        // badTest = manDelta + one + negativeOne = manDelta ---> manDelta = added + deleted = delta
        return new ArithmeticPostAggregation(
                "badTest",
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS,
                asList(
                        new FieldAccessorPostAggregation(
                                new DoubleSumAggregation(
                                        ADDED,
                                        ADDED
                                )
                        ),
                        new FieldAccessorPostAggregation(
                                new DoubleSumAggregation(
                                        DELETED,
                                        DELETED
                                )
                        )
                )
        )
    }
}
