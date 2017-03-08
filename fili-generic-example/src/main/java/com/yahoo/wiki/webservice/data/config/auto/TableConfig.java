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
 * Created by kevin on 3/3/2017.
 */
public class TableConfig implements DruidConfig {
    private String tableName;
    private List<String> metrics;
    private List<String> dimensions;
    private List<TimeGrain> timeGrains;

    public TableConfig(String name) {
        tableName = name;
        metrics = new ArrayList<>();
        dimensions = new ArrayList<>();
        timeGrains = new ArrayList<>();
    }

    public void addMetric(String metric) {
        metrics.add(metric);
    }

    public boolean removeMetric(String metric) {
        return metrics.remove(metric);
    }

    public void addDimension(String dimension) {
        dimensions.add(dimension);
    }

    public boolean removeDimension(String dimension) {
        return dimensions.remove(dimension);
    }

    public void addTimeGrain(TimeGrain t) {
        timeGrains.add(t);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public TableName getTableName() {
        return () -> EnumUtils.camelCase(tableName);
    }

    @Override
    public List<String> getMetrics() {
        return Collections.unmodifiableList(metrics);
    }

    @Override
    public List<String> getDimensions() {
        return Collections.unmodifiableList(dimensions);
    }

    @Override
    public List<TimeGrain> getValidTimeGrains() {
        return timeGrains;
    }
}
