// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

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
import com.yahoo.wiki.webservice.data.config.ExternalConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensionsLoader;

import org.joda.time.DateTimeZone;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Load the Wikipedia-specific table configuration.
 */
public class WikiTableLoader extends BaseTableLoader {

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

    private final Map<String, WikiPhysicalTableInfoTemplate> physicalTableDictionary = new HashMap<>();

    /**
     * Constructor.
     *
     * @param metadataService Service containing the segment data for constructing tables
     */
    public WikiTableLoader(DataSourceMetadataService metadataService) {
        super(metadataService);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param wikiDimensions   The dimensions to load into test tables
     * @param metricDictionary The dictionary to use when looking up metrics for this table
     * @param physicalTables   a set of physical tables
     * @param logicalTables    a set of logical tables
     */
    private void configureSample(WikiDimensionsLoader wikiDimensions, MetricDictionary metricDictionary,
                                 LinkedHashSet<WikiPhysicalTableInfoTemplate> physicalTables,
                                 LinkedHashSet<WikiLogicalTableInfoTemplate> logicalTables
    ) {

        // Map from dimension name to dimension configuration
        Map<String, DimensionConfig> dimensions = wikiDimensions.getDimensionConfigurations();

        // Map from physical table's name to physical table
        for (WikiPhysicalTableInfoTemplate physicalTable : physicalTables) {
            physicalTableDictionary.put(physicalTable.getName(), physicalTable);
        }

        // For each logical table:
        // - Update tableDefinitions (logicalTable to physical table group)
        // - Update validGrains (logicalTable to all possible Granularities)
        // - Update apiMetricNames (logicalTable to apiMetrics)
        // - Update druidMetricNames (logicalTable to druidMetrics)
        for (WikiLogicalTableInfoTemplate logicalTable : logicalTables) {

            LinkedHashSet<FieldName> druidMetrics = new LinkedHashSet<>();
            LinkedHashSet<ApiMetricName> apiMetrics = new LinkedHashSet<>();
            Set<PhysicalTableDefinition> samplePhysicalTableDefinition = new LinkedHashSet<>();

            // For each physical table this logical table depends on:
            // - Add physicalMetrics to druidMetrics
            // - Make samplePhysicalTableDefinition
            // (From physical table's name, ZonedTimeGrain, physicalMetrics and dimensions)
            for (String physicalTableName : logicalTable.getPhysicalTables()) {

                // Get physical table through physical table's name
                WikiPhysicalTableInfoTemplate physicalTable = physicalTableDictionary
                        .get(EnumUtils.camelCase(physicalTableName));

                // Metrics for this physical table
                Set<FieldName> physicalMetrics = physicalTable
                        .getMetrics()
                        .stream()
                        .map(
                                metricName -> metricDictionary.get(metricName)
                        )
                        .collect(Collectors.toSet());

                // Dimensions for this physical table
                Set<DimensionConfig> dimensionSet = physicalTable.getDimensions().stream().map(
                        dimension -> dimensions.get(dimension)
                ).collect(Collectors.toSet());

                // Make a sample physical table definition
                samplePhysicalTableDefinition.add(
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
            tableDefinitions.put(logicalTable, samplePhysicalTableDefinition);
            validGrains.put(logicalTable, logicalTable.getGranularities());

            // Add all api metrics to apiMetrics
            apiMetrics.addAll(logicalTable.getApiMetrics().stream().map(
                    apiMetricNames -> metricDictionary.get(apiMetricNames)
            ).collect(Collectors.toList()));

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

        ExternalConfigLoader tableConfigLoader = new ExternalConfigLoader(new ObjectMapper());
        WikiTableConfigTemplate wikiTableConfigTemplate =
                tableConfigLoader.parseExternalFile(
                        "TableConfigTemplateSample.json",
                        WikiTableConfigTemplate.class);

        LinkedHashSet<WikiPhysicalTableInfoTemplate> physicalTables = wikiTableConfigTemplate.getPhysicalTables();
        LinkedHashSet<WikiLogicalTableInfoTemplate> logicalTables = wikiTableConfigTemplate.getLogicalTables();

        configureSample(new WikiDimensionsLoader(), dictionaries.metric, physicalTables, logicalTables);

        for (WikiLogicalTableInfoTemplate table : logicalTables) {
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
