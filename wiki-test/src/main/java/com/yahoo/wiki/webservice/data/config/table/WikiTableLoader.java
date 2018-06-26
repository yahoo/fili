// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY;
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.HOUR;

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
import com.yahoo.bard.webservice.data.time.AllGranularity;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.ExternalConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensionsLoader;

import org.joda.time.DateTimeZone;

import java.util.*;

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
     * @param tables           a set of tables
     */
    private void configureSample(WikiDimensionsLoader wikiDimensions, MetricDictionary metricDictionary,
                                 LinkedHashSet<WikiTableConfigTemplate> tables) {

        // Dimensions
        Set<DimensionConfig> dimsBasefactDruidTableName = wikiDimensions.getDimensionConfigurationsByConfigInfo();

        for (WikiTableConfigTemplate table : tables) {

            LinkedHashSet<FieldName> druidMetrics = new LinkedHashSet<>();
            LinkedHashSet<ApiMetricName> apiMetrics = new LinkedHashSet<>();

            for (String metric : table.getDruidTable().getMetrics()) {
                druidMetrics.add(metricDictionary.get(metric));
            }
            for (String metric : table.getLogicalTable().getMetrics()) {
                apiMetrics.add(metricDictionary.get(metric));
            }

            druidMetricNames.put(
                    table.getDruidTable(),
                    druidMetrics
            );

            apiMetricNames.put(
                    table.getLogicalTable(),
                    apiMetrics
            );

            Set<PhysicalTableDefinition> samplePhysicalTableDefinition = Utils.asLinkedHashSet(
                    new ConcretePhysicalTableDefinition(
                            table.getDruidTable(),
                            HOUR.buildZonedTimeGrain(DateTimeZone.UTC),
                            druidMetricNames.get(table.getDruidTable()),
                            dimsBasefactDruidTableName
                    )
            );

            tableDefinitions.put(table.getLogicalTable(), samplePhysicalTableDefinition);
            validGrains.put(table.getLogicalTable(), Utils.asLinkedHashSet(HOUR, DAY, AllGranularity.INSTANCE));

        }
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {

        ExternalConfigLoader tableConfigLoader = new ExternalConfigLoader(new ObjectMapper());
        WikiTableSetTemplate wikiTableSetTemplate =
                tableConfigLoader.parseExternalFile(
                        "TableConfigTemplateSample.json",
                        WikiTableSetTemplate.class);

        LinkedHashSet<WikiTableConfigTemplate> tables = wikiTableSetTemplate.getTables();

        configureSample(new WikiDimensionsLoader(), dictionaries.metric, tables);

        for (WikiTableConfigTemplate table : tables) {
            TableGroup tableGroup = buildDimensionSpanningTableGroup(
                    apiMetricNames.get(table.getLogicalTable()),
                    druidMetricNames.get(table.getLogicalTable()),
                    tableDefinitions.get(table.getLogicalTable()),
                    dictionaries
            );
            Set<Granularity> validGranularities =
                    validGrains.get(table.getLogicalTable());
            loadLogicalTableWithGranularities(table.getLogicalTable().getName(),
                    tableGroup, validGranularities, dictionaries);
        }

    }
}
