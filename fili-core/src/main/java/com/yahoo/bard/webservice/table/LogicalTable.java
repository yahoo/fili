// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.config.DefaultLogicalTableInfo;
import com.yahoo.bard.webservice.config.LogicalTableInfo;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.joda.time.ReadablePeriod;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * A LogicalTable has a grain and a tablegroup of physical tables that satisfy the logical table.
 */
public class LogicalTable implements Table, Comparable<LogicalTable> {


    private TableGroup tableGroup;
    private LogicalTableSchema schema;

    private String name;
    private String longName;
    private String description;
    private String category;
    private ReadablePeriod retention;


    // parameter used by the compare to method
    private String comparableParam;

    /**
     * Constructor
     * <p>
     * Sets the Category and Retention to the defaults, and sets the long name and description to the name.
     *
     * @param name  The logical table name
     * @param granularity  The logical table granularity
     * @param tableGroup  The tableGroup for the logical table
     * @param metricDictionary The metric dictionary to bind tableGroup's metrics
     *
     * @deprecated in favor of constructing using LogicalTableInfo with default values
     */
    @Deprecated
    public LogicalTable(
            @NotNull String name,
            @NotNull Granularity granularity,
            TableGroup tableGroup,
            MetricDictionary metricDictionary
    ) {
        this(new DefaultLogicalTableInfo(name), granularity, tableGroup, metricDictionary);
    }

    /**
     * Constructor
     *  A constructor that takes in a logical table info, and uses that information.
     *
     * @param logicalTableInfo  Hello.
     * @param granularity  Hello.
     * @param tableGroup  Hello.
     * @param metricDictionary  Hello.
     */
    public LogicalTable(
            @NotNull LogicalTableInfo logicalTableInfo,
            @NotNull Granularity granularity,
            TableGroup tableGroup,
            MetricDictionary metricDictionary
    ) {
        this(logicalTableInfo.getName(),
                logicalTableInfo.getCategory(),
                logicalTableInfo.getLongName(),
                granularity,
                logicalTableInfo.getRetention().orElse(null),
                logicalTableInfo.getDescription(),
                tableGroup,
                metricDictionary);
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
        this.longName = longName;
        this.description = description;
        this.category = category;
        this.retention = retention;

        this.tableGroup = tableGroup;
        this.comparableParam = name + granularity.toString();
        schema = new LogicalTableSchema(tableGroup, granularity, metricDictionary);
    }

    public TableGroup getTableGroup() {
        return this.tableGroup;
    }

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

    public Granularity getGranularity() {
        return schema.getGranularity();
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
