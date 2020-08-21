// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.LogicalTableName;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import org.joda.time.ReadablePeriod;
import org.joda.time.Years;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * A LogicalTable can be conceived as a view on the underlying physical dataSources that it supports.
 *
 * The LogicalTableSchema defines the grains and columns available on that view.  The viewFilters field provides row
 * filtering on that view.
 *
 * The TableGroup field provides a collection of tables that should, collectively, satisfy the schema of this
 * LogicalTable.  During query planning that collection is scanned and compared for lowest row cardinality.
 */
public class LogicalTable implements Table, Comparable<LogicalTable> {

    public static final String DEFAULT_CATEGORY = "General";
    public static final ReadablePeriod DEFAULT_RETENTION = Years.ONE;

    private String name;

    private String category;
    private String longName;
    private ReadablePeriod retention;
    private String description;

    private TableGroup tableGroup;
    private LogicalTableSchema schema;
    private ApiFilters viewFilters;

    // parameter used by the compare to method
    private String comparableParam;

    /**
     * Constructor
     * <p>
     * Sets the Category and Retention to the defaults, and sets the long name and description to the name.
     *
     * Defaults to the TableGroup for schema and no ApiFilters.
     *
     * @param name  The logical table name
     * @param granularity  The logical table granularity
     * @param tableGroup  The tablegroup for the logical table
     * @param metricDictionary The metric dictionary to bind tableGroup's metrics
     *
     * @deprecated prefer constructor that uses the LogicalTableName class
     */
    @Deprecated
    public LogicalTable(
            @NotNull String name,
            @NotNull Granularity granularity,
            TableGroup tableGroup,
            MetricDictionary metricDictionary
    ) {
        this(LogicalTableName.forName(name), granularity, tableGroup, metricDictionary);
    }

    /**
     * Constructor.
     *
     * Uses the LogicalTableName interface to package metadata.
     *
     * Defaults to TableGroup for schema and no ApiFilters.
     *
     * @param name  The logical table name
     * @param granularity  The logical table time grain
     * @param tableGroup  The tablegroup for the logical table
     * @param metricDictionary The metric dictionary to bind tableGroup's metrics
     */
    public LogicalTable(
            @NotNull LogicalTableName name,
            @NotNull Granularity granularity,
            TableGroup tableGroup,
            MetricDictionary metricDictionary
    ) {
        this(
                name.asName(),
                name.getCategory(),
                name.getLongName(),
                granularity,
                name.getRetention().orElse(null),
                name.getDescription(),
                tableGroup,
                metricDictionary
        );
    }

    /**
     * Constructor.
     *
     * Defaults to the TableGroup schema.
     *
     * @param name  The logical table name
     * @param category  The category of the logical table
     * @param longName  The long name of the logical table
     * @param granularity  The logical table time grain
     * @param retention  The period the data in the logical table is retained for
     * @param description  The description for this logical table
     * @param tableGroup  The tablegroup for the logical table
     * @param metricDictionary The metric dictionary to bind tableGroup's metrics
     *
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
        this(
                name,
                category,
                longName,
                retention,
                description,
                tableGroup,
                new LogicalTableSchema(tableGroup, granularity, metricDictionary),
                tableGroup.getApiFilters()
        );
    }

    /**
     * Copy Constructor.
     *
     * @param name  The logical table name
     * @param category  The category of the logical table
     * @param longName  The long name of the logical table
     * @param retention  The period the data in the logical table is retained for
     * @param description  The description for this logical table
     * @param tableGroup  The tablegroup for the logical table
     * @param schema The LogicalTableSchema backing this LogicalTable
     * @param viewFilters  A list of filters that get attached to any API query sent to this logical table.
     */
    protected LogicalTable(
            @NotNull String name,
            String category,
            String longName,
            ReadablePeriod retention,
            String description,
            TableGroup tableGroup,
            LogicalTableSchema schema,
            ApiFilters viewFilters
    ) {
        this.name = name;
        this.tableGroup = tableGroup;
        this.category = category;
        this.longName = longName;
        this.retention = retention;
        this.description = description;
        this.comparableParam = name + schema.getGranularity().toString();
        this.schema = schema;
        this.viewFilters = viewFilters == null ? null : new ApiFilters(viewFilters);
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

    public Optional<ApiFilters> getFilters() {
        return Optional.ofNullable(viewFilters);
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
