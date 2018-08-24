// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.slurper.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricsLoader;
import com.yahoo.luthier.webservice.data.config.table.ExternalTableLoader;
import com.yahoo.slurper.webservice.data.config.dimension.DimensionSerializer;
import com.yahoo.slurper.webservice.data.config.metric.MetricSerializer;
import com.yahoo.slurper.webservice.data.config.table.TableSerializer;
import com.yahoo.slurper.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.slurper.webservice.data.config.auto.DruidNavigator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Builds dimensions, metrics, and tables for all datasources found from druid.
 */
public class GenericBinderFactory extends AbstractBinderFactory {

    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;

    private static final String DRUID_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/config/druid/";
    private static final String EXTERNAL_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/config/external/";

    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader();
    private ExternalDimensionsLoader externalDimensionsLoader;
    private DimensionSerializer dimensionSerializer;
    private MetricSerializer metricSerializer;

    /**
     * Constructs a GenericBinderFactory using the MetadataDruidWebService
     * to configure dimensions, tables, and metrics from Druid.
     */
    public GenericBinderFactory() {
        DruidWebService druidWebService = buildMetadataDruidWebService(getMappers().getMapper());
        configLoader = new DruidNavigator(druidWebService, getMapper());
    }

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        dimensionSerializer = new DimensionSerializer(new ObjectMapper());
        dimensionSerializer
                .setConfig(configLoader)
                .setPath(DRUID_CONFIG_FILE_PATH)
                .parseToJson();

        externalDimensionsLoader = new ExternalDimensionsLoader(
                externalConfigLoader,
                DRUID_CONFIG_FILE_PATH
        );

        return new LinkedHashSet<>(externalDimensionsLoader.getAllDimensionConfigurations());
    }

    @Override
    protected TableLoader getTableLoader() {
        TableSerializer tableSerializer = new TableSerializer(new ObjectMapper());
        tableSerializer
                .setDimensions(dimensionSerializer)
                .setMetrics(metricSerializer)
                .setConfig(configLoader)
                .setPath(DRUID_CONFIG_FILE_PATH)
                .parseToJson();

        return new ExternalTableLoader(
                dimensionSerializer.getMetadataService(),
                externalDimensionsLoader,
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
    }

    @Override
    protected MetricLoader getMetricLoader() {
        metricSerializer = new MetricSerializer(new ObjectMapper());
        metricSerializer
                .setConfig(configLoader)
                .setPath(DRUID_CONFIG_FILE_PATH)
                .parseToJson();

        return new ExternalMetricsLoader(
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
    }
}
