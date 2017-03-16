// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.auto.ConfigLoader;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensions;
import com.yahoo.wiki.webservice.data.config.metric.MetricNameGenerator;

import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Load the Wikipedia-specific table configuration.
 */
public class GenericTableLoader extends BaseTableLoader {
    private final Map<String, Set<Granularity>> validGrains =
            new HashMap<>();
    // Set up the metrics
    private final Map<String, Set<FieldName>> druidMetricNames =
            new HashMap<>();
    private final Map<String, Set<ApiMetricName>> apiMetricNames =
            new HashMap<>();
    // Set up the table definitions
    private final Map<String, Set<PhysicalTableDefinition>> tableDefinitions =
            new HashMap<>();
    private final ConfigLoader configLoader;

    /**
     * Constructor.
     */
    public GenericTableLoader(ConfigLoader configLoader, GenericDimensions genericDimensions) {
        this.configLoader = configLoader;
        configureTables(genericDimensions);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param genericDimensions  The dimensions to load into test tables.
     */
    private void configureTables(GenericDimensions genericDimensions) {
        configLoader.getTableNames().forEach(dataSourceConfiguration -> {
            TimeGrain defaultTimeGrain = dataSourceConfiguration.getValidTimeGrains()
                    .get(0); //TODO this should probably have it's own method

            Set<DimensionConfig> dimsBasefactDruidTable = getBaseFactDruidTable(
                    genericDimensions,
                    dataSourceConfiguration
            );

            Set<PhysicalTableDefinition> physicalTableDefinitions = getPhysicalTableDefinitions(
                    dataSourceConfiguration,
                    defaultTimeGrain,
                    dimsBasefactDruidTable
            );

            tableDefinitions.put(dataSourceConfiguration.getName(), physicalTableDefinitions);

            druidMetricNames.put(
                    dataSourceConfiguration.getName(),
                    new LinkedHashSet<>(
                            dataSourceConfiguration.getMetrics()
                                    .stream()
                                    .map(MetricNameGenerator::getDruidMetric)
                                    .collect(Collectors.toList())
                    )
            );

            apiMetricNames.put(
                    dataSourceConfiguration.getName(),
                    new LinkedHashSet<>(
                            dataSourceConfiguration.getMetrics()
                                    .stream()
                                    .map(metricName -> MetricNameGenerator.getFiliMetricName(
                                            metricName,
                                            dataSourceConfiguration.getValidTimeGrains()
                                    ))
                                    .collect(Collectors.toList())
                    )
            );

            validGrains.put(
                    dataSourceConfiguration.getName(),
                    getGranularities(dataSourceConfiguration)
            );
        });


    }

    private Set<Granularity> getGranularities(DataSourceConfiguration dataSourceConfiguration) {
        LinkedHashSet<Granularity> granularities = new LinkedHashSet<>(dataSourceConfiguration.getValidTimeGrains());
        granularities.add(AllGranularity.INSTANCE);
        return granularities;
    }

    private Set<PhysicalTableDefinition> getPhysicalTableDefinitions(
            final DataSourceConfiguration dataSourceConfiguration,
            TimeGrain timeGrain,
            final Set<DimensionConfig> dimsBasefactDruidTable
    ) {
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(
                (ZonelessTimeGrain) timeGrain,
                DateTimeZone.UTC
        );
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        dataSourceConfiguration.getTableName(),
                        zonedTimeGrain,
                        dimsBasefactDruidTable
                )
        );
    }

    private Set<DimensionConfig> getBaseFactDruidTable(
            GenericDimensions genericDimensions,
            DataSourceConfiguration dataSourceConfiguration
    ) {
        return genericDimensions.getDimensionConfigurationsByApiName(
                (String[]) dataSourceConfiguration.getDimensions().toArray()
        );
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        configLoader.getTableNames()
                .forEach(table -> {
                    TableGroup tableGroup = buildTableGroup(
                            table.getTableName().asName(),
                            apiMetricNames.get(table.getName()),
                            druidMetricNames.get(table.getName()),
                            tableDefinitions.get(table.getName()),
                            dictionaries
                    );
                    Set<Granularity> validGranularities = validGrains.get(table.getName());
                    loadLogicalTableWithGranularities(
                            table.getTableName().asName(),
                            tableGroup,
                            validGranularities,
                            dictionaries
                    );
                });
    }
}
