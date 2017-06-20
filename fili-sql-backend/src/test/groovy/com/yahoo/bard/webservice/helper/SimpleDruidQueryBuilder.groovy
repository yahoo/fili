// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.helper

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
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.having.Having
import com.yahoo.bard.webservice.druid.model.orderby.TopNMetric
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation
import com.yahoo.bard.webservice.druid.model.query.GroupByQuery
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.druid.model.query.TopNQuery
import com.yahoo.bard.webservice.metadata.DataSourceMetadata
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.StrictPhysicalTable
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.Utils

import org.joda.time.DateTimeZone
import org.joda.time.Interval

import java.util.stream.Collectors

class SimpleDruidQueryBuilder {
    public static final String WIKITICKER = "WIKITICKER"
    public static final String IS_NEW = "IS_NEW"
    public static final String IS_ROBOT = "IS_ROBOT"
    public static final String PAGE = "PAGE"
    public static final String USER = "USER"
    public static final String COMMENT = "COMMENT"
    public static final String ADDED = "ADDED";
    public static final String DELETED = "DELETED";
    public static final String DELTA = "DELTA";
    public static final String ID = "ID"
    public static final String METRO_CODE = "METRO_CODE"
    public static final String START = "2015-09-12T00:00:00.000Z"
    public static final String END = "2015-09-13T00:00:00.000Z"

    private Simple() {
    }


    public static TimeSeriesQuery timeSeriesQuery(
            String name,
            Filter filter,
            DefaultTimeGrain timeGrain,
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
            DefaultTimeGrain timeGrain,
            List<String> metrics,
            List<String> dimensions,
            List<Aggregation> aggregations,
            List<PostAggregation> postAggs,
            List<Interval> intervals
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
                null //todo what goes here
        )
    }

    public static TopNQuery topNQuery(
            String name,
            Filter filter,
            int threshold,
            String orderByMetric,
            Dimension groupByDimension,
            DefaultTimeGrain timeGrain,
            List<String> metrics,
            List<String> dimensions,
            List<Aggregation> aggregations,
            List<PostAggregation> postAggs,
            List<Interval> intervals
    ) {
        return new TopNQuery(
                dataSource(name, metrics, dimensions),
                timeGrain,
                groupByDimension,
                filter,
                aggregations,
                postAggs,
                intervals,
                threshold,
                new TopNMetric(orderByMetric)
        )
    }

    public static DataSource dataSource(String name, List<String> metrics, List<String> dimensions) {
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(DefaultTimeGrain.DAY, DateTimeZone.UTC);
        Set<Column> columns = setOf();
        Map<String, String> logicalToPhysicalColumnNames = Collections.emptyMap();

        DataSourceMetadataService metadataService = new DataSourceMetadataService();
        metadataService.update(
                DataSourceName.of(name),
                new DataSourceMetadata(name, Collections.emptyMap(), Collections.emptyList())
        );

        Collection<String> metricsAndDimensions = new ArrayList<>(metrics);
        metricsAndDimensions.addAll(dimensions);
        return new TableDataSource(
                new ConstrainedTable(
                        new StrictPhysicalTable(
                                TableName.of(name),
                                zonedTimeGrain,
                                columns,
                                logicalToPhysicalColumnNames,
                                metadataService
                        ),
                        new DataSourceConstraint(
                                setOf(), // create dimensions to test grouping those
                                setOf(getDimensions(dimensions)),
                                setOf(),
                                setOf(metrics),
                                setOf(),
                                setOf(dimensions),
                                setOf(metricsAndDimensions),
                                Collections.emptyMap()
                        )
                )
        );
    }

    public static List<Dimension> getDimensions(String... dimensions) {
        return getDimensions(Arrays.asList(dimensions))
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
                        dimension,
                        "General",
                        Utils.asLinkedHashSet(DefaultDimensionField.ID),
                        MapStoreManager.getInstance(dimension),
                        ScanSearchProviderManager.getInstance(dimension)
                )
        )
    }

    public static <T> Set<T> setOf(T... e) {
        return e == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(e));
    }

    public static <T> Set<T> setOf(List<T> e) {
        return new HashSet<T>(e);
    }

    public static ArithmeticPostAggregation arithmeticPA() {
        // badTest = manDelta + one + negativeOne = manDelta ---> manDelta = added + deleted = delta
        return new ArithmeticPostAggregation(
                "badTest",
                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS,
                Arrays.asList(
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
