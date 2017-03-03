package com.yahoo.wiki.webservice.data.config.auto;

import com.yahoo.bard.webservice.data.time.TimeGrain;

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

    public TableConfig(String name) {
        tableName = name;
        metrics = new ArrayList<>();
        dimensions = new ArrayList<>();
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

    public String getName() {
        return tableName;
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
        //TODO: get interval, split("/"), load as joda time, get difference, estimate valid time grain
        throw new AssertionError("Can't detect valid grains yet");
    }

    public void clear() {
        metrics.clear();
        dimensions.clear();
    }
}
