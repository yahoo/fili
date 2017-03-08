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
import com.yahoo.wiki.webservice.data.config.auto.DruidConfig;
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
    private ConfigLoader configLoader;

    /**
     * Constructor.
     */
    public GenericTableLoader(ConfigLoader configLoader) {
        GenericDimensions genericDimensions = new GenericDimensions(configLoader);
        this.configLoader = configLoader;
        configureSample(genericDimensions);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param genericDimensions  The dimensions to load into test tables.
     */
    private void configureSample(GenericDimensions genericDimensions) {

        for (DruidConfig druidConfig : configLoader.getTableNames()) {
            TimeGrain defaultTimeGrain = druidConfig.getValidTimeGrains().get(0);

            Set<DimensionConfig> dimsBasefactDruidTable = getBaseFactDruidTable(genericDimensions, druidConfig);

            Set<PhysicalTableDefinition> physicalTableDefinitions = getPhysicalTableDefinitions(
                    druidConfig,
                    defaultTimeGrain,
                    dimsBasefactDruidTable
            );

            tableDefinitions.put(druidConfig.getName(), physicalTableDefinitions);

            MetricNameGenerator.setDefaultTimeGrain(defaultTimeGrain);

            druidMetricNames.put(
                    druidConfig.getName(),
                    new LinkedHashSet<>(
                            druidConfig.getMetrics()
                                    .stream()
                                    .map(MetricNameGenerator::getDruidMetric)
                                    .collect(Collectors.toList())
                    )
            );

            apiMetricNames.put(
                    druidConfig.getName(),
                    new LinkedHashSet<>(
                            druidConfig.getMetrics()
                                    .stream()
                                    .map(MetricNameGenerator::getFiliMetricName)
                                    .collect(Collectors.toList())
                    )
            );

            validGrains.put(
                    druidConfig.getName(),
                    getGranularities(druidConfig)
            );

        }

    }

    private Set<Granularity> getGranularities(DruidConfig druidConfig) {
        LinkedHashSet<Granularity> granularities = new LinkedHashSet<>(druidConfig.getValidTimeGrains());
        granularities.add(AllGranularity.INSTANCE);
        return granularities;
    }

    private Set<PhysicalTableDefinition> getPhysicalTableDefinitions(
            final DruidConfig druidConfig,
            TimeGrain timeGrain,
            final Set<DimensionConfig> dimsBasefactDruidTable
    ) {
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(
                (ZonelessTimeGrain) timeGrain,
                DateTimeZone.UTC
        );
        return Utils.asLinkedHashSet(
                new PhysicalTableDefinition(
                        druidConfig.getTableName(),
                        zonedTimeGrain,
                        dimsBasefactDruidTable
                )
        );
    }

    private Set<DimensionConfig> getBaseFactDruidTable(GenericDimensions genericDimensions, DruidConfig druidConfig) {
        return genericDimensions.getDimensionConfigurationsByApiName(
                (String[]) druidConfig.getDimensions().toArray()
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
