// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.joda.time.ReadablePeriod;
import org.joda.time.Years;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * A LogicalTable has a grain and a tablegroup of physical tables that satisfy the logical table.
 */
public class LogicalTable implements Table, Comparable<LogicalTable> {

    public static final String DEFAULT_CATEGORY = "General";
    public static final ReadablePeriod DEFAULT_RETENTION = Years.ONE;

    private TableGroup tableGroup;
    private LogicalTableSchema schema;
    private Granularity granularity;
    // parameter used by the compare to method
    private String name;
    private String comparableParam;
    private String category;
    private String longName;
    private ReadablePeriod retention;
    private String description;

    /**
     * Constructor
     * <p>
     * Sets the Category and Retention to the defaults, and sets the long name and description to the name.
     *
     * @param name  The logical table name
     * @param granularity  The logical table granularity
     * @param tableGroup  The tablegroup for the logical table
     * @param metricDictionary The metric dictionary to bind tableGroup's metrics
     */
    public LogicalTable(
            @NotNull String name,
            @NotNull Granularity granularity,
            TableGroup tableGroup,
            MetricDictionary metricDictionary
    ) {
        this(name, DEFAULT_CATEGORY, name, granularity, DEFAULT_RETENTION, name, tableGroup, metricDictionary);
    }

    /**
     * Constructor.
     *
     * @param name  The logical table name
     * @param category  The category of the logical table
     * @param longName  The long name of the logical table
     * @param granularity  The logical table time grain
     * @param retention  The period the data in the logical table is retained for
     * @param description  The description for this logical table
     * @param tableGroup  The tablegroup for the logical table
     * @param metricDictionary The metric dictionary to bind tableGroup's metrics
     */
    public LogicalTable(
            @NotNull String name,
            String category,
            String longName,
            @NotNull Granularity granularity,
            ReadablePeriod retention,
            String description,
            TableGroup tableGroup,
            MetricDictionary metricDictionary
    ) {
        this.name = name;
        this.tableGroup = tableGroup;
        this.category = category;
        this.longName = longName;
        this.retention = retention;
        this.description = description;
        this.granularity = granularity;
        this.comparableParam = name + granularity.toString();

        schema = new LogicalTableSchema(tableGroup, granularity, metricDictionary);

    }

    /**
     * Getter for table group.
     *
     * @return tableGroup
     */
    public TableGroup getTableGroup() {
        return this.tableGroup;
    }

    /**
     * Getter for logical metrics.
     *
     * @return set of LogicalMetric
     */
    public Set<LogicalMetric> getLogicalMetrics() {
        return schema.getColumns(LogicalMetricColumn.class).stream()
                .map(LogicalMetricColumn::getLogicalMetric)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public int compareTo(LogicalTable o) {
        return (this.comparableParam).compareTo(o.comparableParam);
    }

    public String getCategory() {
        return category;
    }

    public String getLongName() {
        return longName;
    }

    public ReadablePeriod getRetention() {
        return retention;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Getter.
     *
     * @return The granularity
     */
    public Granularity getGranularity() {
        return granularity;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public LogicalTableSchema getSchema() {
        return schema;
    }
}
