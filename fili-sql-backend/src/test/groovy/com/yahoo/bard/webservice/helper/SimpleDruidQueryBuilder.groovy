// Copyright 2016 Yahoo Inc.
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
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation
import com.yahoo.bard.webservice.druid.model.datasource.DataSource
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource
import com.yahoo.bard.webservice.druid.model.filter.Filter
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.ConstantPostAggregation
import com.yahoo.bard.webservice.druid.model.postaggregation.FieldAccessorPostAggregation
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery
import com.yahoo.bard.webservice.metadata.DataSourceMetadata
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.ConstrainedTable
import com.yahoo.bard.webservice.table.StrictPhysicalTable
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.Utils

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval

/**
 * Created by hinterlong on 6/7/17.
 */
class SimpleDruidQueryBuilder {

    private static final String METRIC1 = "ADDED";
    private static final String METRIC2 = "DELETED";
    private static final String METRIC3 = "DELTA";
    private static final String DIMENSION1 = "COMMENT";

    private Simple() {
    }

    public static TimeSeriesQuery timeSeriesQuery(String name, Filter filter = null, DefaultTimeGrain timeGrain) {
        return new TimeSeriesQuery(
                dataSource(name),
                timeGrain,
                filter,
                Arrays.asList(
                        new DoubleSumAggregation(METRIC1, METRIC1),
                        new DoubleSumAggregation(METRIC2, METRIC2),
                        new DoubleSumAggregation(METRIC3, METRIC3)
                ),
                Arrays.asList(
                        // badTest = manDelta + one + negativeOne = manDelta ---> manDelta = added + deleted = delta
                        new ArithmeticPostAggregation(
                                "badTest",
                                ArithmeticPostAggregation.ArithmeticPostAggregationFunction.PLUS,
                                Arrays.asList(
                                        new FieldAccessorPostAggregation(
                                                new DoubleSumAggregation(
                                                        METRIC1,
                                                        METRIC1
                                                )
                                        ),
                                        new FieldAccessorPostAggregation(
                                                new DoubleSumAggregation(
                                                        METRIC2,
                                                        METRIC2
                                                )
                                        )
                                )
                        )
                ),
                Arrays.asList(
                        new Interval(
                                DateTime.parse("2015-09-12T00:00:00.000Z"),
                                DateTime.parse("2015-09-15T00:50:00.000Z")
                        )
                )
        );
    }

    public static DataSource dataSource(String name) {
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(DefaultTimeGrain.DAY, DateTimeZone.UTC);
        Set<Column> columns = setOf();
        Map<String, String> logicalToPhysicalColumnNames = Collections.emptyMap();

        DataSourceMetadataService metadataService = new DataSourceMetadataService();
        metadataService.update(
                DataSourceName.of(name),
                new DataSourceMetadata(name, Collections.emptyMap(), Collections.emptyList())
        );

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
                                setOf(getDimension(DIMENSION1)),
                                setOf(),
                                setOf(METRIC1, METRIC2, METRIC3),
                                setOf(),
                                setOf(DIMENSION1),
                                setOf(METRIC1, METRIC2, METRIC3, DIMENSION1),
                                Collections.emptyMap()
                        )
                )
        );
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

}
