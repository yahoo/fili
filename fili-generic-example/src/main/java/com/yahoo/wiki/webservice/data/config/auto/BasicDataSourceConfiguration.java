// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;

import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * BasicDataSourceConfiguration to hold all metrics, dimensions, timegrains, and the name of a datasource.
 */
public class BasicDataSourceConfiguration implements DataSourceConfiguration {
    private final String tableName;
    private final List<String> metrics;
    private final List<String> dimensions;
    private TimeGrain timeGrain;

    /**
     * Construct the BasicDataSourceConfiguration from a name.
     *
     * @param name  Name of the BasicDataSourceConfiguration.
     */
    public BasicDataSourceConfiguration(String name) {
        tableName = name;
        metrics = new ArrayList<>();
        dimensions = new ArrayList<>();
    }

    /**
     * Add a metric to the datasource.
     *
     * @param metric  Name of metric to hold in BasicDataSourceConfiguration.
     */
    public void addMetric(String metric) {
        metrics.add(metric);
    }

    /**
     * Add a dimension to the datasource.
     *
     * @param dimension  Name of dimension to hold in the BasicDataSourceConfiguration.
     */
    public void addDimension(String dimension) {
        dimensions.add(dimension);
    }

    /**
     * Add a {@link TimeGrain} to the datasource.
     *
     * @param timeGrain  Valid Timegrain to hold in the BasicDataSourceConfiguration.
     */
    public void setTimeGrain(TimeGrain timeGrain) {
        this.timeGrain = timeGrain;
    }

    /**
     * Gets the name of the table.
     *
     * @return the name of the table.
     */
    @Override
    public String getPhysicalTableName() {
        return tableName;
    }

    @Override
    public String getApiTableName() {
        return getPhysicalTableName();
    }

    /**
     * Gets the metrics from the datasource.
     *
     * @return the names of metrics stored in BasicDataSourceConfiguration.
     */
    @Override
    public List<String> getMetrics() {
        return Collections.unmodifiableList(metrics);
    }

    /**
     * Gets the dimensions from the datasource.
     *
     * @return the names of the dimensions stored in the BasicDataSourceConfiguration.
     */
    @Override
    public List<String> getDimensions() {
        return Collections.unmodifiableList(dimensions);
    }

    @Override
    public ZonedTimeGrain getZonedTimeGrain() {
        return new ZonedTimeGrain(
                (ZonelessTimeGrain) timeGrain,
                DateTimeZone.UTC
        );
    }

    @Override
    public List<TimeGrain> getValidTimeGrains() {
        return Collections.singletonList(timeGrain);
    }

}
