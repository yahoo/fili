// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import static com.yahoo.bard.webservice.database.Database.ADDED
import static com.yahoo.bard.webservice.database.Database.CHANNEL
import static com.yahoo.bard.webservice.database.Database.CITY_NAME
import static com.yahoo.bard.webservice.database.Database.COMMENT
import static com.yahoo.bard.webservice.database.Database.COUNTRY_ISO_CODE
import static com.yahoo.bard.webservice.database.Database.COUNTRY_NAME
import static com.yahoo.bard.webservice.database.Database.DELETED
import static com.yahoo.bard.webservice.database.Database.DELTA
import static com.yahoo.bard.webservice.database.Database.IS_ANONYMOUS
import static com.yahoo.bard.webservice.database.Database.IS_MINOR
import static com.yahoo.bard.webservice.database.Database.IS_NEW
import static com.yahoo.bard.webservice.database.Database.IS_ROBOT
import static com.yahoo.bard.webservice.database.Database.IS_UNPATROLLED
import static com.yahoo.bard.webservice.database.Database.METRO_CODE
import static com.yahoo.bard.webservice.database.Database.NAMESPACE
import static com.yahoo.bard.webservice.database.Database.PAGE
import static com.yahoo.bard.webservice.database.Database.REGION_ISO_CODE
import static com.yahoo.bard.webservice.database.Database.REGION_NAME
import static com.yahoo.bard.webservice.database.Database.SCHEMA
import static com.yahoo.bard.webservice.database.Database.TIME
import static com.yahoo.bard.webservice.database.Database.USER
import static com.yahoo.bard.webservice.database.Database.WIKITICKER
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
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.orderby.LimitSpec
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.druid.model.orderby.SortDirection
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.data.time.Granularity
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.metadata.DataSourceMetadata
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.sql.ApiToFieldMapper
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.PhysicalTableDictionary
import com.yahoo.bard.webservice.table.SqlPhysicalTable
import com.yahoo.bard.webservice.table.availability.PermissiveAvailability
import com.yahoo.bard.webservice.table.resolver.BaseDataSourceConstraint
import com.yahoo.bard.webservice.util.Utils
import com.yahoo.bard.webservice.web.filters.ApiFilters

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
        physicalTableDictionary.put(WIKITICKER, dataSource.physicalTable.sourceTable as ConfigPhysicalTable)
        return physicalTableDictionary
    }

    public static TableDataSource getWikitickerDatasource(String apiPrepend, String fieldPrepend) {
        return dataSource(
                WIKITICKER,
                DefaultTimeGrain.DAY,
                DateTimeZone.UTC,
                [ADDED, DELETED, DELTA],
                [
                        COUNTRY_ISO_CODE, IS_NEW, IS_ROBOT, PAGE,
                        USER, COMMENT, IS_UNPATROLLED, NAMESPACE,
                        COUNTRY_NAME, CITY_NAME, IS_MINOR, IS_ANONYMOUS,
                        REGION_ISO_CODE, CHANNEL, REGION_NAME, METRO_CODE
                ],
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
        )
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

    public static LimitSpec getSort(List<String> columns, List<SortDirection> sortDirections) {
        return getSort(columns, sortDirections, Optional.empty())
    }

    public static LimitSpec getSort(List<String> columns, List<SortDirection> sortDirections, Optional<Integer> limit) {
        LinkedHashSet<OrderByColumn> sorts = []
        for (int i = 0; i < columns.size(); i++) {
            sorts.add(
                    new OrderByColumn(columns.get(i), sortDirections.get(i))
            )
        }

        return new LimitSpec(sorts, limit)
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
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(zonelessTimeGrain, dateTimeZone)
        Set<Column> columns = [] as Set
        Map<String, String> logicalToPhysicalColumnNames = [:]
        metrics.forEach { logicalToPhysicalColumnNames.put(apiPrepend + it, fieldPrepend + it) }
        dimensions.forEach { logicalToPhysicalColumnNames.put(apiPrepend + it, fieldPrepend + it) }

        DataSourceMetadataService metadataService = new DataSourceMetadataService();
        metadataService.update(
                DataSourceName.of(name),
                new DataSourceMetadata(name, [:], [])
        );

        dimensions = dimensions.collect { apiPrepend + it }
        metrics = metrics.collect { apiPrepend + it }

        Set<String> metricsAndDimensions = new HashSet<>()
        metrics.forEach{ metricsAndDimensions.add(apiPrepend + it) }
        dimensions.forEach{ metricsAndDimensions.add(apiPrepend + it) }
        def strictPhysicalTable = new SqlPhysicalTable(
                TableName.of(name),
                zonedTimeGrain,
                columns,
                logicalToPhysicalColumnNames,
                new PermissiveAvailability(DataSourceName.of(name), metadataService), //todo correct?
                SCHEMA,
                TIME
        )

        return new TableDataSource(
                new ConstrainedTable(
                        strictPhysicalTable,
                        new BaseDataSourceConstraint(
                                [] as Set,
                                [] as Set,
                                [] as Set,
                                metrics as Set,
                                getDimensions(dimensions) as Set,
                                dimensions as Set,
                                metricsAndDimensions,
                                new ApiFilters()
                        )
                )
        )
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
}
