// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConcretePhysicalTable;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.Schema;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.table.TableIdentifier;

import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides commonly-needed methods for loading tables.
 */
public abstract class BaseTableLoader implements TableLoader {

    private static final Logger LOG = LoggerFactory.getLogger(BaseTableLoader.class);

    protected final DateTimeZone defaultTimeZone;

    /**
     * A table loader using a time context and a default time zone.
     *
     * @param defaultTimeZone  The default time zone to tables being loaded
     */
    protected BaseTableLoader(DateTimeZone defaultTimeZone) {
        this.defaultTimeZone = defaultTimeZone;
    }

    /**
     * A table loader using a standard time context and UTC default time zone.
     */
    protected BaseTableLoader() {
        this(DateTimeZone.UTC);
    }

    @Override
    public abstract void loadTableDictionary(ResourceDictionaries dictionaries);

    /**
     * Builds a table group.
     * <p>
     * Builds and loads the physical tables for the physical table definitions as well.
     *
     * @param logicalTableName  The logical table for the table group
     * @param apiMetrics  The set of metric names surfaced to the api
     * @param druidMetrics  Names of druid datasource metric columns
     * @param tableDefinitions  A list of config objects for physical tables
     * @param dictionaries  The container for all the data dictionaries
     *
     * @return A table group binding all the tables for a logical table view together.
     *
     * @deprecated logicalTableName is not used in TableGroup
     */
    @Deprecated
    public TableGroup buildTableGroup(
            String logicalTableName,
            Set<ApiMetricName> apiMetrics,
            Set<FieldName> druidMetrics,
            Set<PhysicalTableDefinition> tableDefinitions,
            ResourceDictionaries dictionaries
    ) {
        return buildTableGroup(apiMetrics, druidMetrics, tableDefinitions, dictionaries);
    }

    /**
     * Builds a table group.
     * <p>
     * Builds and loads the physical tables for the physical table definitions as well.
     *
     * @param apiMetrics  The set of metric names surfaced to the api
     * @param druidMetrics  Names of druid datasource metric columns
     * @param tableDefinitions  A list of config objects for physical tables
     * @param dictionaries  The container for all the data dictionaries
     *
     * @return A table group binding all the tables for a logical table view together.
     */
    public TableGroup buildTableGroup(
            Set<ApiMetricName> apiMetrics,
            Set<FieldName> druidMetrics,
            Set<PhysicalTableDefinition> tableDefinitions,
            ResourceDictionaries dictionaries
    ) {
        // Load a physical table for each of the table definitions
        LinkedHashSet<PhysicalTable> physicalTables = new LinkedHashSet<>();
        for (PhysicalTableDefinition def : tableDefinitions) {
            PhysicalTable table;
            table = loadPhysicalTable(def, druidMetrics, dictionaries);
            physicalTables.add(table);
        }

        //Derive the logical dimensions by taking the union of all the physical dimensions
        Set<Dimension> dimensions = physicalTables.stream()
                .map(PhysicalTable::getSchema)
                .map(Schema::getColumns)
                .flatMap(Set::stream)
                .filter(column -> column instanceof DimensionColumn)
                .map(column -> (DimensionColumn) column)
                .map(DimensionColumn::getDimension)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return new TableGroup(physicalTables, apiMetrics, dimensions);
    }

    /**
     * Load several logical tables into the logicalDictionary.
     * <p>
     * Note: This builds the logical tables as well.
     *
     * @param nameGroupMap  A map of logical table name to table group information
     * @param validGrains  The accepted grains for the logical table
     * @param dictionaries  The resource dictionaries for reading and storing configuration
     */
    public void loadLogicalTablesWithGranularities(
            Map<String, TableGroup> nameGroupMap,
            Set<? extends Granularity> validGrains,
            ResourceDictionaries dictionaries
    ) {
        // For every logical table name, group pair
        for (Map.Entry<String, TableGroup> entry : nameGroupMap.entrySet()) {
            String logicalTableName = entry.getKey();
            TableGroup group = entry.getValue();

            loadLogicalTableWithGranularities(logicalTableName, group, validGrains, dictionaries);
        }
    }

    /**
     * Load a logical table into the logicalDictionary.
     * <p>
     * Note: This builds the logical table as well.
     *
     * @param logicalTableName  The logical table name
     * @param nameGroup  The table group information for the logical table
     * @param validGrains  The accepted grains for the logical table
     * @param dictionaries  The resource dictionaries for reading and storing configuration
     */
    public void loadLogicalTableWithGranularities(
            String logicalTableName,
            TableGroup nameGroup,
            Set<? extends Granularity> validGrains,
            ResourceDictionaries dictionaries
    ) {
        LogicalTableDictionary logicalDictionary = dictionaries.getLogicalDictionary();
        MetricDictionary metricDictionary = dictionaries.getMetricDictionary();

        // For every legal grain
        for (Granularity grain : validGrains) {
            // Build the logical table
            LogicalTable logicalTable = buildLogicalTable(logicalTableName, grain, nameGroup, metricDictionary);

            // Load it into the dictionary
            logicalDictionary.put(new TableIdentifier(logicalTable), logicalTable);
        }
    }

    /**
     * Build a logical table, supplying it with a name, grain, table group, and metrics, the default category and
     * longName set to the name.
     * <p>
     * Note: This builds a logical table with all valid metrics for the grain of the table
     *
     * @param name  The name for this logical table
     * @param granularity  The granularity for this logical table
     * @param group  The group of physical tables for this logical table
     * @param metrics  The dictionary of all metrics
     *
     * @return The logical table built
     */
    public LogicalTable buildLogicalTable(
            String name,
            Granularity granularity,
            TableGroup group,
            MetricDictionary metrics
    ) {
        return buildLogicalTable(
                name,
                granularity,
                LogicalTable.DEFAULT_CATEGORY,
                name,
                LogicalTable.DEFAULT_RETENTION,
                name,
                group,
                metrics
        );
    }

    /**
     * Build a logical table, supplying it with a name, grain, table group, and metrics.
     * <p>
     * Note: This builds a logical table with all valid metrics for the grain of the table
     *
     * @param name  The name for this logical table
     * @param granularity  The granularity for this logical table
     * @param category  The category for this logical table
     * @param longName  The long name for this logical table
     * @param retention  The retention for this logical table
     * @param description  The description for this logical table
     * @param group  The group of physical tables for this logical table
     * @param metrics  The dictionary of all metrics
     *
     * @return The logical table built
     */
    public LogicalTable buildLogicalTable(
            String name,
            Granularity granularity,
            String category,
            String longName,
            ReadablePeriod retention,
            String description,
            TableGroup group,
            MetricDictionary metrics
    ) {

        // All Logical tables support the dimension set for their table group
        PhysicalTable firstPhysicalTable = group.getPhysicalTables().iterator().next();
        Set<PhysicalTable> tables = group.getPhysicalTables();
        for (Dimension dim : group.getDimensions()) {
            // Select the first table with a non-default logical mapping to this dimension name
            // otherwise, use the defaulting behavior from the first table in the list
            PhysicalTable physicalTable = tables.stream()
                    .filter(table -> table.hasLogicalMapping(dim.getApiName()))
                    .findFirst()
                    .orElse(firstPhysicalTable);

        }

        LogicalTable logicalTable = new LogicalTable(
                name,
                category,
                longName,
                granularity,
                retention,
                description,
                group,
                metrics
        );

        return logicalTable;
    }

    /**
     * Load a new physical table into the dictionary and return the loaded physical table.
     *
     * @param definition  A config object for the physical table
     * @param metricNames  The Set of metric names on the table
     * @param dictionaries  The resource dictionaries for reading and storing resource data
     *
     * @return The physical table created
     */
    protected PhysicalTable loadPhysicalTable(
            PhysicalTableDefinition definition,
            Set<FieldName> metricNames,
            ResourceDictionaries dictionaries
    ) {
        PhysicalTable existingTable = dictionaries.getPhysicalDictionary().get(definition.getName().asName());
        if (existingTable == null) {
            // Build the physical table
            existingTable = buildPhysicalTable(definition, metricNames, dictionaries.getDimensionDictionary());

            // Add the table to the dictionary
            LOG.debug("Physical table: {} \n\n" + "Cache: {} ",
                            definition.getName().asName(),
                            existingTable.getColumns());
            dictionaries.getPhysicalDictionary().put(definition.getName().asName(), existingTable);
        }

        return existingTable;
    }

    /**
     * Build a physical table, populating columns and initializing the cache.
     *
     * @param definition  A config object for the physical table
     * @param metricNames  Set of Druid metrics on the table
     * @param dimensionDictionary  The dimension dictionary
     *
     * @return The physical table, configured per the parameters
     */
    protected PhysicalTable buildPhysicalTable(
            PhysicalTableDefinition definition,
            Set<FieldName> metricNames,
            DimensionDictionary dimensionDictionary
    ) {
        // Load the dimension columns
        LinkedHashSet<Column> columns = definition.getDimensions().stream()
                .map(DimensionConfig::getApiName)
                .map(dimensionDictionary::findByApiName)
                .map(DimensionColumn::new)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // And the metric columns
        columns.addAll(
                metricNames.stream()
                    .map(FieldName::asName)
                    .map(MetricColumn::new)
                    .collect(Collectors.toList())
        );

        return new ConcretePhysicalTable(
                definition.getName(),
                definition.getGrain(),
                columns,
                definition.getLogicalToPhysicalNames()
        );
    }
}
