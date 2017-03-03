package com.yahoo.wiki.webservice.data.config.auto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by kevin on 3/3/2017.
 */
public class TableConfig {
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

    public List<String> getMetrics() {
        return Collections.unmodifiableList(metrics);
    }

    public List<String> getDimensions() {
        return Collections.unmodifiableList(dimensions);
    }

    public void clear() {
        metrics.clear();
        dimensions.clear();
    }
}
