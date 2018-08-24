// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Load the Wikipedia-specific table configuration.
 */
public class ExternalTableLoader extends BaseTableLoader {

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

    private final Map<String, PhysicalTableDefinition> physicalTableMap = new HashMap<>();
    private final Map<String, Set<FieldName>> physicalTableToMetricsMap = new HashMap<>();

    private ExternalConfigLoader tableConfigLoader;
    private String externalConfigFilePath;
    private ExternalDimensionsLoader externalDimensionsLoader;

    /**
     * Constructor using the default external configuration loader.
     * and default external configuration file path.
     *
     * @param metadataService Service containing the segment data for constructing tables
     * @param externalDimensionsLoader external dimension loader
     * @param tableConfigLoader The external configuration loader for loading tables
     * @param externalConfigFilePath The external file's url containing the external config information
     */
    public ExternalTableLoader(
            DataSourceMetadataService metadataService,
            ExternalDimensionsLoader externalDimensionsLoader,
            ExternalConfigLoader tableConfigLoader,
            String externalConfigFilePath
    ) {
        super(metadataService);
        this.externalDimensionsLoader = externalDimensionsLoader;
        this.tableConfigLoader = tableConfigLoader;
        this.externalConfigFilePath = externalConfigFilePath;
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param physicalTables   a set of physical tables
     * @param logicalTables    a set of logical tables
     */
    private void configureTables(Set<PhysicalTableInfoTemplate> physicalTables,
                                 Set<LogicalTableInfoTemplate> logicalTables
    ) {

        // Map from dimension name to dimension configuration
        Map<String, DimensionConfig> dimensionsMap = externalDimensionsLoader.getDimensionConfigurations();

        physicalTables.forEach(physicalTable -> {
            physicalTableMap.put(physicalTable.getName(), physicalTable.build(dimensionsMap));
            physicalTableToMetricsMap.put(
                    physicalTable.getName(),
                    physicalTable.getMetrics()
                            .stream().map(metric -> (FieldName) () -> (metric))
                            .collect(Collectors.toSet()));
        });

        logicalTables.forEach(logicalTable -> {
            Set<FieldName> druidMetrics = new HashSet<>();
            Set<PhysicalTableDefinition> physicalTableDefinition = new HashSet<>();

            // Add all api metrics to apiMetrics
            Set<ApiMetricName> apiMetrics = logicalTable.getApiMetrics().stream()
                    .map(ApiMetricName::of)
                    .collect(Collectors.toSet());

            TableName logicalTableName = TableName.of(logicalTable.getName());

            logicalTable.getPhysicalTables().forEach(physicalTableName -> {
                physicalTableDefinition.add(physicalTableMap.get(physicalTableName));
                druidMetrics.addAll(physicalTableToMetricsMap.get(physicalTableName));
            });

            tableDefinitions.put(logicalTableName, physicalTableDefinition);
            validGrains.put(logicalTableName, logicalTable.getGranularities());
            apiMetricNames.put(logicalTableName, apiMetrics);
            druidMetricNames.put(logicalTableName, druidMetrics);
        });
    }

    /**
     * Templates and deserializers binder.
     *
     * @return A Joda Module contains binding information.
     */
    private JodaModule bindTemplates() {
        JodaModule jodaModule = new JodaModule();
        jodaModule.addAbstractTypeMapping(ExternalTableConfigTemplate.class,
                DefaultExternalTableConfigTemplate.class);
        jodaModule.addAbstractTypeMapping(LogicalTableInfoTemplate.class,
                DefaultLogicalTableInfoTemplate.class);
        jodaModule.addAbstractTypeMapping(PhysicalTableInfoTemplate.class,
                DefaultPhysicalTableInfoTemplate.class);
        return jodaModule;
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {

        JodaModule jodaModule = bindTemplates();
        ObjectMapper objectMapper = new ObjectMapper().registerModule(jodaModule);

        ExternalTableConfigTemplate tableConfig =
                tableConfigLoader.parseExternalFile(
                        externalConfigFilePath + "TableConfig.json",
                        ExternalTableConfigTemplate.class, objectMapper);

        Set<PhysicalTableInfoTemplate> physicalTables = tableConfig.getPhysicalTables();
        Set<LogicalTableInfoTemplate> logicalTables = tableConfig.getLogicalTables();

        configureTables(physicalTables, logicalTables);

        logicalTables.forEach(table -> {
            TableName tableName = TableName.of(table.getName());
            TableGroup tableGroup = buildDimensionSpanningTableGroup(
                    apiMetricNames.get(tableName),
                    druidMetricNames.get(tableName),
                    tableDefinitions.get(tableName),
                    dictionaries
            );
            loadLogicalTableWithGranularities(table.getName(),
                    tableGroup, validGrains.get(tableName), dictionaries);
        });

    }
}
