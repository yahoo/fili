// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.ConcretePhysicalTableDefinition;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadata;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.auto.MetricConfig;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensionConfigs;
import com.yahoo.wiki.webservice.data.config.metric.DruidMetricName;
import com.yahoo.wiki.webservice.data.config.metric.FiliApiMetricName;

import org.joda.time.DateTimeZone;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Load the table configuration for any druid setup.
 */
public class GenericTableLoader extends BaseTableLoader {
    private final Map<String, Set<Granularity>> dataSourceToValidGrains = new HashMap<>();
    // Set up the metrics
    private final Map<String, Set<FieldName>> dataSourceToDruidMetricNames = new HashMap<>();
    private final Map<String, Set<ApiMetricName>> dataSourceToApiMetricNames = new HashMap<>();
    // Set up the table definitions
    private final Map<String, Set<PhysicalTableDefinition>> dataSourceToTableDefinitions = new HashMap<>();
    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;

    /**
     * Constructor.
     *
     * @param configLoader  Gives a list of {@link DataSourceConfiguration} to build tables from.
     * @param metadataService  Service containing the segment data for constructing tables.
     */
    public GenericTableLoader(
            @NotNull Supplier<List<? extends DataSourceConfiguration>> configLoader,
            DataSourceMetadataService metadataService
    ) {
        super(metadataService);
        this.configLoader = configLoader;
        configureTables(metadataService);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param metadataService  The metadata service to plays the datasources in.
     */
    private void configureTables(
            DataSourceMetadataService metadataService
    ) {
        configLoader.get().forEach(dataSourceConfiguration -> {

            metadataService.update(
                    DataSourceName.of(dataSourceConfiguration.getApiTableName()),
                    new DataSourceMetadata(
                            dataSourceConfiguration.getApiTableName(),
                            Collections.emptyMap(),
                            Collections.emptyList()
                    )
            );

            dataSourceToDruidMetricNames.put(
                    dataSourceConfiguration.getApiTableName(),
                    dataSourceConfiguration.getMetricConfigs()
                            .stream()
                            .map(MetricConfig::getDruidMetricName)
                            .collect(Collectors.toSet())
            );

            dataSourceToTableDefinitions.put(
                    dataSourceConfiguration.getApiTableName(),
                    getPhysicalTableDefinitions(
                            dataSourceConfiguration,
                            dataSourceConfiguration.getZonedTimeGrain()
                    )
            );

            dataSourceToApiMetricNames.put(
                    dataSourceConfiguration.getApiTableName(),
                    dataSourceConfiguration.getMetricConfigs()
                            .stream()
                            .map(MetricConfig::getFiliApiMetricName)
                            .collect(Collectors.toSet())
            );

            dataSourceToValidGrains.put(
                    dataSourceConfiguration.getApiTableName(),
                    dataSourceConfiguration.getGranularities()
            );
        });

    }

    /**
     * Creates a {@link PhysicalTableDefinition} definitions.
     *
     * @param dataSourceConfiguration  DataSourceConfiguration to build physical table definition from.
     * @param timeGrain  Valid timegrain for table to be created.
     *
     * @return set of PhysicalTableDefinition for the datasource.
     */
    private Set<PhysicalTableDefinition> getPhysicalTableDefinitions(
            DataSourceConfiguration dataSourceConfiguration,
            ZonedTimeGrain timeGrain
    ) {
        return Utils.asLinkedHashSet(
                new ConcretePhysicalTableDefinition(
                        dataSourceConfiguration.getTableName(),
                        timeGrain,
                        dataSourceToDruidMetricNames.get(dataSourceConfiguration.getApiTableName()),
                        dataSourceConfiguration.getDimensionConfigs()
                )
        );
    }

    /**
     * Load each logical table from the datasources given.
     *
     * @param dictionaries  ResourceDictionaries to load with each table.
     */
    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        configLoader.get()
                .forEach(table -> {
                    Set<TableName> currentTableGroupTableNames = dataSourceToTableDefinitions
                            .get(table.getApiTableName())
                            .stream()
                            .map(PhysicalTableDefinition::getName)
                            .collect(Collectors.toSet());

                    TableGroup tableGroup = buildDimensionSpanningTableGroup(
                            currentTableGroupTableNames,
                            dataSourceToTableDefinitions.get(table.getApiTableName()),
                            dictionaries,
                            dataSourceToApiMetricNames.get(table.getApiTableName())
                    );

                    loadLogicalTableWithGranularities(
                            table.getTableName().asName(),
                            tableGroup,
                            dataSourceToValidGrains.get(table.getApiTableName()),
                            dictionaries
                    );
                });
    }
}
