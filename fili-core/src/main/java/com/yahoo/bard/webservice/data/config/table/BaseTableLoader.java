// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.table.TableIdentifier;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides commonly-needed methods for loading tables.
 */
public abstract class BaseTableLoader implements TableLoader {

    private static final Logger LOG = LoggerFactory.getLogger(BaseTableLoader.class);

    protected final DateTimeZone defaultTimeZone;

    private final DataSourceMetadataService metadataService;

    /**
     * A table loader using a standard time context and UTC default time zone.
     *
     * @param metadataService  Service containing the segment data for constructing tables
     */
    protected BaseTableLoader(DataSourceMetadataService metadataService) {
        this(DateTimeZone.UTC, metadataService);
    }

    /**
     * A table loader using a time context and a default time zone.
     *
     * @param defaultTimeZone  The default time zone to tables being loaded
     * @param metadataService  Service containing the segment data for constructing tables
     */
    protected BaseTableLoader(DateTimeZone defaultTimeZone, DataSourceMetadataService metadataService) {
        this.defaultTimeZone = defaultTimeZone;
        this.metadataService = metadataService;
    }

    /**
     * Load user configured tables into resource dictionary.
     *
     * @param dictionaries dictionary to be loaded with configured tables
     */
    @Override
    public abstract void loadTableDictionary(ResourceDictionaries dictionaries);

    /**
     * Builds a table group that derive its dimensions by taking the union of all the underlying physical dimensions.
     * <p>
     * Builds and loads the physical tables from table definitions for the current table group.
     *
     * @param currentTableGroupTableNames  Set of table name of tables belonging to this table group
     * @param tableDefinitions  A list of config objects for building physical tables and its dependent physical tables
     * @param dictionaries  The container for all the data dictionaries
     * @param apiMetrics  The set of metric names surfaced to the api
     *
     * @return A table group binding all the tables for a logical table view together.
     */
    public TableGroup buildDimensionSpanningTableGroup(
            Set<TableName> currentTableGroupTableNames,
            Set<PhysicalTableDefinition> tableDefinitions,
            ResourceDictionaries dictionaries,
            Set<ApiMetricName> apiMetrics
    ) {
        Map<String, PhysicalTableDefinition> availableTableDefinitions = buildPhysicalTableDefinitionDictionary(
                tableDefinitions
        );

        // Get the physical table from physical table dictionary, if not exist, build it and put it in dictionary
        LinkedHashSet<PhysicalTable> physicalTables = currentTableGroupTableNames.stream()
                .map(TableName::asName)
                .map(tableName -> buildPhysicalTableWithDependency(
                                        tableName,
                                        availableTableDefinitions,
                                        dictionaries
                        )
                ).collect(Collectors.toCollection(LinkedHashSet::new));

        // Derive the dimensions by taking the union of all the physical dimensions
        Set<Dimension> dimensions = physicalTables.stream()
                .map(PhysicalTable::getSchema)
                .map(schema -> schema.getColumns(DimensionColumn.class))
                .flatMap(Set::stream)
                .map(DimensionColumn::getDimension)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return new TableGroup(physicalTables, apiMetrics, dimensions);
    }

    /**
     * Load several logical tables into the logicalDictionary, all with the globally-scoped metric dictionary.
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
        loadLogicalTablesWithGranularities(
                nameGroupMap,
                validGrains,
                dictionaries.getLogicalDictionary(),
                nameGroupMap.keySet().stream()
                        .collect(Collectors.toMap(Function.identity(), i -> dictionaries.getMetricDictionary()))
        );
    }

    /**
     * Load several logical tables into the logicalDictionary, each with their own scoped metric dictionary.
     * <p>
     * Note: This builds the logical tables as well.
     *
     * @param nameGroupMap  A map of logical table name to table group information
     * @param validGrains  The accepted grains for the logical table
     * @param tableDictionary  The logical table dictionary to be populated
     * @param scopedMetrics  A mapping from table name to the scoped MetricDictionary to use for that table
     */
    public void loadLogicalTablesWithGranularities(
            Map<String, TableGroup> nameGroupMap,
            Set<? extends Granularity> validGrains,
            LogicalTableDictionary tableDictionary,
            Map<String, MetricDictionary> scopedMetrics
    ) {
        for (Map.Entry<String, TableGroup> entry : nameGroupMap.entrySet()) {
            String logicalTableName = entry.getKey();
            TableGroup group = entry.getValue();

            loadLogicalTableWithGranularities(
                    logicalTableName,
                    group,
                    validGrains,
                    tableDictionary,
                    scopedMetrics.get(logicalTableName)
            );
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
        loadLogicalTableWithGranularities(
                logicalTableName,
                nameGroup,
                validGrains,
                dictionaries.getLogicalDictionary(),
                dictionaries.getMetricDictionary()
        );
    }

    /**
     * Load a logical table into the logicalDictionary.
     * <p>
     * Note: This builds the logical table as well.
     *
     * @param logicalTableName  The logical table name
     * @param nameGroup  The table group information for the logical table
     * @param validGrains  The accepted grains for the logical table
     * @param tableDictionary  The dictionary to load the logical table into
     * @param metricDictionary  The dictionary to use when looking up metrics for this table
     */
    public void loadLogicalTableWithGranularities(
            String logicalTableName,
            TableGroup nameGroup,
            Set<? extends Granularity> validGrains,
            LogicalTableDictionary tableDictionary,
            MetricDictionary metricDictionary
    ) {
        // For every legal grain
        for (Granularity grain : validGrains) {
            // Build the logical table
            LogicalTable logicalTable = new LogicalTable(logicalTableName, grain, nameGroup, metricDictionary);
            // Load it into the dictionary
            tableDictionary.put(new TableIdentifier(logicalTable), logicalTable);
        }
    }

    /**
     * Build and return the current physical table given its table name and definition, if dependency exists, build its
     * dependencies and load the dependencies into physical table dictionary.
     * <p>
     * note: current physical table not loaded into physical table dictionary, only dependencies will
     *
     * @param currentTableName  Table name of the table being built
     * @param availableTableDefinitions  A map of table name to table definition that are available globally
     * @param dictionaries  Contains both dimension and physical table dictionary for building and dependency resolution
     *
     * @return the current physical table built from table definitions
     */
    protected ConfigPhysicalTable buildPhysicalTableWithDependency(
            String currentTableName,
            Map<String, PhysicalTableDefinition> availableTableDefinitions,
            ResourceDictionaries dictionaries
    ) {
        PhysicalTableDictionary physicalTableDictionary = dictionaries.getPhysicalDictionary();

        // Check if table is already built
        if (physicalTableDictionary.containsKey(currentTableName)) {
            return physicalTableDictionary.get(currentTableName);
        }

        // Remove the current definition from map so that we don't try to circle back to it if circular dependency exist
        PhysicalTableDefinition currentTableDefinition = availableTableDefinitions.remove(currentTableName);

        // If the table definition is currently being build or missing
        if (Objects.isNull(currentTableDefinition)) {
            String message = String.format(
                    "Unable to resolve physical table dependency for physical table: %s, might be missing or " +
                            "circular dependency", currentTableName);

            LOG.error(message);
            throw new IllegalStateException(message);
        }

        // If dependent table not in physical table dictionary, try to recursively build it and put it in the dictionary
        currentTableDefinition.getDependentTableNames()
                .forEach(
                        tableName -> {
                            if (!physicalTableDictionary.containsKey(tableName.asName())) {
                                physicalTableDictionary.put(
                                        tableName.asName(),
                                        buildPhysicalTableWithDependency(
                                                tableName.asName(),
                                                availableTableDefinitions,
                                                dictionaries
                                        )
                                );
                            }
                        }
                );

        LOG.debug("Table Loader Building Physical Table {}");

        // Build the current physical table using physical table dictionary to resolve dependency
        ConfigPhysicalTable currentTableBuilt = currentTableDefinition.build(
                dictionaries,
                getDataSourceMetadataService()
        );
        physicalTableDictionary.put(currentTableName, currentTableBuilt);

        return currentTableBuilt;
    }

    /**
     * Getter for the data source metadata service use for creating physical tables.
     *
     * @return the data source metadata service associated with the table loader
     */
    protected DataSourceMetadataService getDataSourceMetadataService() {
        return metadataService;
    }

    /**
     * Build a map from physical table name to its table definition.
     *
     * @param physicalTableDefinitions  Definitions to build the map from
     *
     * @return the map of physical table name to its table definition
     */
    private Map<String, PhysicalTableDefinition> buildPhysicalTableDefinitionDictionary(
            Set<PhysicalTableDefinition> physicalTableDefinitions
    ) {
        return physicalTableDefinitions.stream()
                .collect(Collectors.toMap(definition -> definition.getName().asName(), Function.identity()));
    }

    /**
     * Load a new physical table into the dictionary and return the loaded physical table.
     *
     * @param definition  A config object for the physical table
     * @param metricNames  The Set of metric names on the table
     * @param dictionaries  The resource dictionaries for reading and storing resource data
     *
     * @return The physical table created
     *
     * @deprecated use buildPhysicalTableWithDependency instead, which also supports building table with dependencies
     */
    @Deprecated
    protected PhysicalTable loadPhysicalTable(
            PhysicalTableDefinition definition,
            Set<FieldName> metricNames,
            ResourceDictionaries dictionaries
    ) {
        LOG.debug(
                "Building table {} with deprecated loadPhysicalTable method, use buildPhysicalTableWithDependency " +
                        "instead",
                definition.getName().asName()
        );
        return definition.build(dictionaries, getDataSourceMetadataService());
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
     *
     * @deprecated does not load table with external dependency, use the other buildDimensionSpanningTableGroup instead
     */
    @Deprecated
    public TableGroup buildDimensionSpanningTableGroup(
            Set<ApiMetricName> apiMetrics,
            Set<FieldName> druidMetrics,
            Set<PhysicalTableDefinition> tableDefinitions,
            ResourceDictionaries dictionaries
    ) {
        Set<TableName> currentTableGroupTableNames = tableDefinitions.stream()
                .map(PhysicalTableDefinition::getName)
                .collect(Collectors.toSet());

        return buildDimensionSpanningTableGroup(
                currentTableGroupTableNames,
                tableDefinitions,
                dictionaries,
                apiMetrics
        );
    }
}
