// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import io.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * TableConfig to hold all metrics, dimensions, valid time grain, and the name of a datasource.
 */
public class TableConfig implements DataSourceConfiguration {
    private final String tableName;
    private final Set<String> metrics;
    private final Set<String> dimensions;
    private final List<DataSegment> dataSegments;
    private TimeGrain timeGrain;

    /**
     * Construct the TableConfig from a name.
     *
     * @param name  Name of the TableConfig.
     */
    public TableConfig(String name) {
        tableName = name;
        metrics = new HashSet<>();
        dimensions = new HashSet<>();
        dataSegments = new ArrayList<>();
    }

    /**
     * Add a metric to the datasource.
     *
     * @param metric  Name of metric to hold in TableConfig.
     */
    public void addMetric(String metric) {
        metrics.add(metric);
    }

    /**
     * Add a dimension to the datasource.
     *
     * @param dimension  Name of dimension to hold in the TableConfig.
     */
    public void addDimension(String dimension) {
        dimensions.add(dimension);
    }

    /**
     * Sets {@link TimeGrain} of the datasource.
     *
     * @param timeGrain  Valid Timegrain to hold in the TableConfig.
     */
    public void setTimeGrain(TimeGrain timeGrain) {
        this.timeGrain = timeGrain;
    }

    /**
     * Add a {@link DataSegment} to the existing known datasegments for this table.
     *
     * @param dataSegment  The {@link DataSegment} given by Druid.
     */
    public void addDataSegment(DataSegment dataSegment) {
        dataSegments.add(dataSegment);
    }

    /**
     * Gets the name of the table.
     *
     * @return the name of the table.
     */
    @Override
    public String getName() {
        return tableName;
    }

    /**
     * Gets the {@link TableName} of the current datasource.
     *
     * @return the TableName for the TableConfig.
     */
    @Override
    public TableName getTableName() {
        return this::getName;
    }

    /**
     * Gets the metrics from the datasource.
     *
     * @return the names of metrics stored in TableConfig.
     */
    @Override
    public Set<String> getMetrics() {
        return Collections.unmodifiableSet(metrics);
    }

    /**
     * Gets the dimensions from the datasource.
     *
     * @return the names of the dimensions stored in the TableConfig.
     */
    @Override
    public Set<String> getDimensions() {
        return Collections.unmodifiableSet(dimensions);
    }

    /**
     * Gets the valid TimeGrains for the datasource.
     *
     * @return the valid TimeGrains stored in the TableConfig.
     */
    @Override
    public TimeGrain getValidTimeGrain() {
        return timeGrain;
    }

    @Override
    public List<DataSegment> getDataSegments() {
        return Collections.unmodifiableList(dataSegments);
    }
}
