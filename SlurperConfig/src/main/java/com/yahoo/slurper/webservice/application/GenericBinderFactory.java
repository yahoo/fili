// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.application.DimensionValueLoadTask;
import com.yahoo.bard.webservice.application.DruidDimensionValueLoader;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricsLoader;
import com.yahoo.slurper.webservice.DimensionSerializer;
import com.yahoo.slurper.webservice.MetricSerializer;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.slurper.webservice.data.config.auto.DruidNavigator;
import com.yahoo.slurper.webservice.data.config.dimension.GenericDimensionConfigs;
import com.yahoo.slurper.webservice.data.config.table.GenericTableLoader;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Builds dimensions, metrics, and tables for all datasources found from druid.
 */
public class GenericBinderFactory extends AbstractBinderFactory {
    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;
    private GenericDimensionConfigs genericDimensionConfigs;

    private static final String EXTERNAL_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/config/";
    private static final String DRUID_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/";
    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader();
    private ExternalDimensionsLoader externalDimensionsLoader;

    /**
     * Constructs a GenericBinderFactory using the MetadataDruidWebService
     * to configure dimensions, tables, and metrics from Druid.
     */
    public GenericBinderFactory() {
        DruidWebService druidWebService = buildMetadataDruidWebService(getMappers().getMapper());
        configLoader = new DruidNavigator(druidWebService, getMapper());
//        genericDimensionConfigs = new GenericDimensionConfigs(configLoader);
    }

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        DimensionSerializer dimensionSerializer = new DimensionSerializer(new ObjectMapper());
        dimensionSerializer
                .setConfig(configLoader)
                .setPath("DimensionConfig.json")
                .parseToJson();

        externalDimensionsLoader = new ExternalDimensionsLoader(
                externalConfigLoader,
                DRUID_CONFIG_FILE_PATH
        );

        genericDimensionConfigs = new GenericDimensionConfigs(configLoader);
        return new LinkedHashSet<>(externalDimensionsLoader.getAllDimensionConfigurations());
    }

    @Override
    protected TableLoader getTableLoader() {
        return new GenericTableLoader(configLoader, genericDimensionConfigs, getDataSourceMetadataService());
    }

    @Override
    protected DimensionValueLoadTask buildDruidDimensionsLoader(
            DruidWebService webService,
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary
    ) {
        List<String> dimensionsList = getDimensionConfigurations().stream()
                .map(DimensionConfig::getApiName)
                .collect(Collectors.toList());

        DruidDimensionValueLoader druidDimensionRowProvider = new DruidDimensionValueLoader(
                physicalTableDictionary,
                dimensionDictionary,
                dimensionsList,
                webService
        );

        return new DimensionValueLoadTask(Collections.singletonList(druidDimensionRowProvider));
    }

    @Override
    protected MetricLoader getMetricLoader() {
        MetricSerializer metricSerializer = new MetricSerializer(new ObjectMapper());
        metricSerializer
                .setConfig(configLoader)
                .setPath("MetricConfig.json")
                .parseToJson();

        return new ExternalMetricsLoader(
                externalConfigLoader,
                DRUID_CONFIG_FILE_PATH
        );
    }
}
