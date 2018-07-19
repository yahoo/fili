// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.ConcretePhysicalTableDefinition;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Load the Wikipedia-specific table configuration.
 */
public class TablesLoader extends BaseTableLoader {

    private final Map<TableName, Set<Granularity>> validGrains =
            new HashMap<>();

    // Set up the metrics
    private final Map<TableName, Set<FieldName>> druidMetricNames =
            new HashMap<>();
    private final Map<TableName, Set<ApiMetricName>> apiMetricNames =
            new HashMap<>();

    // Set up the table definitions
    private final Map<TableName, Set<PhysicalTableDefinition>> tableDefinitions =
            new HashMap<>();

    private final Map<String, PhysicalTableInfoTemplate> physicalTableDictionary = new HashMap<>();

    private ExternalConfigLoader tableConfigLoader;
    private String externalConfigFilePath;

    /**
     * Constructor using the default external configuration loader.
     * and default external configuration file path.
     *
     * @param metadataService Service containing the segment data for constructing tables
     */
    public TablesLoader(DataSourceMetadataService metadataService) {
        super(metadataService);
    }

    /**
     * Set up external configuration file path and external configuration loader.
     *
     * @param externalConfigFilePath The external file's url containing the external config information
     */
    public void setUp(String externalConfigFilePath) {
        setUp(new ExternalConfigLoader(), externalConfigFilePath);
    }

    /**
     * Set up external configuration file path and external configuration loader.
     *
     * @param tableConfigLoader The external configuration loader for loading tables
     * @param externalConfigFilePath The external file's url containing the external config information
     */
    public void setUp(ExternalConfigLoader tableConfigLoader,
                      String externalConfigFilePath) {
        this.tableConfigLoader = tableConfigLoader;
        this.externalConfigFilePath = externalConfigFilePath;
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param externalDimensionsLoader   The dimensions to load into test tables
     * @param metricDictionary The dictionary to use when looking up metrics for this table
     * @param physicalTables   a set of physical tables
     * @param logicalTables    a set of logical tables
     */
    private void configureSample(ExternalDimensionsLoader externalDimensionsLoader, MetricDictionary metricDictionary,
                                 LinkedHashSet<PhysicalTableInfoTemplate> physicalTables,
                                 LinkedHashSet<LogicalTableInfoTemplate> logicalTables
    ) {

        // Map from dimension name to dimension configuration
        Map<String, DimensionConfig> dimensions = externalDimensionsLoader.getDimensionConfigurations();

        // Map from physical table's name to physical table
        for (PhysicalTableInfoTemplate physicalTable : physicalTables) {
            physicalTableDictionary.put(physicalTable.getName(), physicalTable);
        }

        // For each logical table:
        // - Update tableDefinitions (logicalTable to physical table group)
        // - Update validGrains (logicalTable to all possible Granularities)
        // - Update apiMetricNames (logicalTable to apiMetrics)
        // - Update druidMetricNames (logicalTable to druidMetrics)
        for (LogicalTableInfoTemplate logicalTable : logicalTables) {

            LinkedHashSet<FieldName> druidMetrics = new LinkedHashSet<>();
            LinkedHashSet<ApiMetricName> apiMetrics = new LinkedHashSet<>();
            Set<PhysicalTableDefinition> physicalTableDefinition = new LinkedHashSet<>();

            // For each physical table this logical table depends on:
            // - Add physicalMetrics to druidMetrics
            // - Make samplePhysicalTableDefinition
            // (From physical table's name, ZonedTimeGrain, physicalMetrics and dimensions)
            for (String physicalTableName : logicalTable.getPhysicalTables()) {

                // Get physical table through physical table's name
                PhysicalTableInfoTemplate physicalTable = physicalTableDictionary
                        .get(EnumUtils.camelCase(physicalTableName));

                // Metrics for this physical table
                Set<FieldName> physicalMetrics = physicalTable
                        .getMetrics()
                        .stream()
                        .map(ApiMetricName::of)
                        .collect(Collectors.toSet());

                // Dimensions for this physical table
                Set<DimensionConfig> dimensionSet = physicalTable
                        .getDimensions().stream()
                        .map(dimensions::get)
                        .collect(Collectors.toSet());

                // Make a sample physical table definition
                physicalTableDefinition.add(
                        new ConcretePhysicalTableDefinition(
                                physicalTable,
                                physicalTable.getGranularity().buildZonedTimeGrain(DateTimeZone.UTC),
                                physicalMetrics,
                                dimensionSet
                        )
                );

                // Add all physical metrics to druidMetrics
                druidMetrics.addAll(physicalMetrics);
            }

            // Update tableDefinitions and validGrains
            tableDefinitions.put(logicalTable, physicalTableDefinition);
            validGrains.put(logicalTable, logicalTable.getGranularities());

            // Add all api metrics to apiMetrics
            apiMetrics.addAll(logicalTable.getApiMetrics().stream()
                    .map(ApiMetricName::of)
                    .collect(Collectors.toList()));

            // Update apiMetricNames and druidMetricNames
            apiMetricNames.put(
                    logicalTable,
                    apiMetrics
            );

            druidMetricNames.put(
                    logicalTable,
                    druidMetrics
            );
        }
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {

        TableConfigTemplate tableConfigTemplate =
                tableConfigLoader.parseExternalFile(
                        externalConfigFilePath + "TableConfig.json",
                        TableConfigTemplate.class, new ObjectMapper());

        LinkedHashSet<PhysicalTableInfoTemplate> physicalTables = tableConfigTemplate.getPhysicalTables();
        LinkedHashSet<LogicalTableInfoTemplate> logicalTables = tableConfigTemplate.getLogicalTables();

        configureSample(new ExternalDimensionsLoader(externalConfigFilePath),
                dictionaries.metric, physicalTables, logicalTables);

        for (LogicalTableInfoTemplate table : logicalTables) {
            TableGroup tableGroup = buildDimensionSpanningTableGroup(
                    apiMetricNames.get(table),
                    druidMetricNames.get(table),
                    tableDefinitions.get(table),
                    dictionaries
            );
            Set<Granularity> validGranularities =
                    validGrains.get(table);
            loadLogicalTableWithGranularities(table.getName(),
                    tableGroup, validGranularities, dictionaries);
        }

    }
}
