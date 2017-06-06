// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.mock;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.aggregation.DoubleSumAggregation;
import com.yahoo.bard.webservice.druid.model.datasource.DataSource;
import com.yahoo.bard.webservice.druid.model.datasource.TableDataSource;
import com.yahoo.bard.webservice.druid.model.query.TimeSeriesQuery;
import com.yahoo.bard.webservice.metadata.DataSourceMetadata;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConstrainedTable;
import com.yahoo.bard.webservice.table.StrictPhysicalTable;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by hinterlong on 5/31/17.
 */
public class Simple {

    private static final String METRIC1 = "ADDED";
    private static final String METRIC2 = "DELETED";

    private Simple() {

    }

    public static DruidResponse<TimeseriesResult> druidResponse() {
        DruidResponse<TimeseriesResult> mockResponse = new DruidResponse<>();
        TimeseriesResult time1 = new TimeseriesResult(DateTime.now());
        time1.add("sample_name1", 0d);
        time1.add("sample_name2", 1d);
        mockResponse.results.add(time1);
        TimeseriesResult time2 = new TimeseriesResult(DateTime.now());
        time2.add("sample_name3", 2d);
        time2.add("sample_name4", 3d);
        mockResponse.results.add(time2);
        return mockResponse;
    }

    public static TimeSeriesQuery timeSeriesQuery(String name) {
        return new TimeSeriesQuery(
                dataSource(name),
                DefaultTimeGrain.DAY,
                null,
                Arrays.asList(
                        new DoubleSumAggregation(METRIC1, METRIC1),
                        new DoubleSumAggregation(METRIC2, METRIC2)
                ),
                Collections.emptyList(),
                Arrays.asList(
                        new Interval(
                                DateTime.parse("2015-09-12T00:00:00.000Z"),
                                DateTime.parse("2015-09-15T00:50:00.000Z")
                        )
                )
        );
    }

    private static DataSource dataSource(String name) {

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
                                setOf(),
                                setOf(),
                                setOf(),
                                setOf(METRIC1, METRIC2),
                                setOf(),
                                setOf(),
                                setOf(METRIC1, METRIC2),
                                Collections.emptyMap()
                        )
                )
        );
    }

    /**
     *
     * @param e
     * @param <T>
     * @return
     */
    private static <T> Set<T> setOf(T... e) {
        return e == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(e));
    }
}
