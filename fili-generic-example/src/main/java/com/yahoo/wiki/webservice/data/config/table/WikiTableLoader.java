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
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensions;
import com.yahoo.wiki.webservice.data.config.metric.DruidMetricName;
import com.yahoo.wiki.webservice.data.config.metric.FiliMetricName;
import com.yahoo.wiki.webservice.data.config.metric.MetricNameGenerator;

import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Load the Wikipedia-specific table configuration.
 */
public class WikiTableLoader extends BaseTableLoader {
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
    public WikiTableLoader(ConfigLoader configLoader) {
        WikiDimensions wikiDimensions = new WikiDimensions(configLoader);
        this.configLoader = configLoader;
        configureSample(wikiDimensions);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param wikiDimensions  The dimensions to load into test tables.
     */
    private void configureSample(WikiDimensions wikiDimensions) {

        for (DruidConfig druidConfig : configLoader.getTableNames()) {
            TimeGrain defaultTimeGrain = druidConfig.getValidTimeGrains().get(0);

            Set<DimensionConfig> dimsBasefactDruidTable = getBaseFactDruidTable(wikiDimensions, druidConfig);

            Set<PhysicalTableDefinition> physicalTableDefinitions = getPhysicalTableDefinitions(
                    druidConfig,
                    defaultTimeGrain,
                    dimsBasefactDruidTable
            );

            tableDefinitions.put(druidConfig.getName(), physicalTableDefinitions);

            MetricNameGenerator.setDefaultTimeGrain(defaultTimeGrain);

            druidMetricNames.put(
                    druidConfig.getName(),
                    Utils.<FieldName>asLinkedHashSet(getDruidMetricNames(druidConfig))
            );

            apiMetricNames.put(
                    druidConfig.getName(),
                    Utils.asLinkedHashSet(getFiliMetricNames(druidConfig))
            );

            validGrains.put(
                    druidConfig.getName(),
                    Utils.asLinkedHashSet(
                            getAllGranularities(druidConfig)
                    )
            );
        }

    }

    //TODO check AllGranularity.INSTANCE
    private Granularity[] getAllGranularities(final DruidConfig druidConfig) {
        List<TimeGrain> timeGrains = druidConfig.getValidTimeGrains();
        Granularity[] granularities = new Granularity[timeGrains.size() + 1];
        for (int i = 0; i < timeGrains.size(); i++) {
            granularities[i] = timeGrains.get(i);
        }
        granularities[granularities.length - 1] = AllGranularity.INSTANCE;
        return granularities;
    }

    private FiliMetricName[] getFiliMetricNames(final DruidConfig druidConfig) {
        List<String> metrics = druidConfig.getMetrics();
        FiliMetricName[] filiMetrics = new FiliMetricName[metrics.size()];
        for (int i = 0; i < filiMetrics.length; i++) {
            filiMetrics[i] = MetricNameGenerator.getFiliMetricName(metrics.get(i));
        }
        return filiMetrics;
    }

    private DruidMetricName[] getDruidMetricNames(final DruidConfig druidConfig) {
        List<String> metrics = druidConfig.getMetrics();
        DruidMetricName[] druidMetrics = new DruidMetricName[metrics.size()];
        for (int i = 0; i < druidMetrics.length; i++) {
            druidMetrics[i] = MetricNameGenerator.getDruidMetric(metrics.get(i));
        }
        return druidMetrics;
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

    private Set<DimensionConfig> getBaseFactDruidTable(WikiDimensions wikiDimensions, DruidConfig druidConfig) {
        return wikiDimensions.getDimensionConfigurationsByApiName(
                (String[]) druidConfig.getDimensions().toArray()
        );
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        for (DruidConfig table : configLoader.getTableNames()) {
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
        }
    }
}
