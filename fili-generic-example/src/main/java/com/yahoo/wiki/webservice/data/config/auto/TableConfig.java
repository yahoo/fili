// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TableConfig to hold all metrics, dimensions, timegrains, and the name of a datasource.
 */
public class TableConfig implements DataSourceConfiguration {
    private final String tableName;
    private final List<String> metrics;
    private final List<String> dimensions;
    private final List<TimeGrain> timeGrains;

    /**
     * Construct the TableConfig from a name.
     * @param name  Name of the TableConfig.
     */
    public TableConfig(String name) {
        tableName = name;
        metrics = new ArrayList<>();
        dimensions = new ArrayList<>();
        timeGrains = new ArrayList<>();
    }

    /**
     * Add a metric to the datasource.
     * @param metric  Name of metric to hold in TableConfig.
     */
    public void addMetric(String metric) {
        metrics.add(metric);
    }

    /**
     * Add a dimension to the datasource.
     * @param dimension  Name of dimension to hold in the TableConfig.
     */
    public void addDimension(String dimension) {
        dimensions.add(dimension);
    }

    /**
     * Add a {@link TimeGrain} to the datasource.
     * @param timeGrain  Valid Timegrain to hold in the TableConfig.
     */
    public void addTimeGrain(TimeGrain timeGrain) {
        timeGrains.add(timeGrain);
    }

    /**
     * Gets the name of the table.
     * @return the name of the table.
     */
    @Override
    public String getName() {
        return tableName;
    }

    /**
     * Gets the {@link TableName} of the current datasource.
     * @return the TableName for the TableConfig.
     */
    @Override
    public TableName getTableName() {
        return this::getName;
    }

    /**
     * Gets the metrics from the datasource.
     * @return the names of metrics stored in TableConfig.
     */
    @Override
    public List<String> getMetrics() {
        return Collections.unmodifiableList(metrics);
    }

    /**
     * Gets the dimensions from the datasource.
     * @return the names of the dimensions stored in the TableConfig.
     */
    @Override
    public List<String> getDimensions() {
        return Collections.unmodifiableList(dimensions);
    }

    /**
     * Gets the valid TimeGrains for the datasource.
     * @return the valid TimeGrains stored in the TableConfig.
     */
    @Override
    public List<TimeGrain> getValidTimeGrains() {
        return timeGrains;
    }
}
