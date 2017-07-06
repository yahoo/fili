// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.application.DruidDimensionsLoader;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator;
import com.yahoo.wiki.webservice.data.config.auto.FileTableConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensionConfigs;
import com.yahoo.wiki.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.GenericTableLoader;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Builds dimensions, metrics, and tables for all datasources found from druid.
 */
public class GenericBinderFactory extends AbstractBinderFactory {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String TABLE_CONFIG = SYSTEM_CONFIG.getPackageVariableName("table_config");
    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;

    /**
     * Constructs a GenericBinderFactory using the MetadataDruidWebService
     * to configure dimensions, tables, and metrics from Druid.
     */
    public GenericBinderFactory() {
        String tableConfigFile = SYSTEM_CONFIG.getStringProperty(TABLE_CONFIG);
        DruidWebService druidWebService = buildMetadataDruidWebService(getMappers().getMapper());
        if (tableConfigFile.isEmpty()) {
            configLoader = new DruidNavigator(druidWebService);
        } else {
            try {
                configLoader = new FileTableConfigLoader(tableConfigFile);
            } catch (IOException e) {
                throw new RuntimeException("Couldn't load configuration file " + tableConfigFile, e);
            }
        }
    }

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        return new GenericDimensionConfigs(configLoader).getAllDimensionConfigurations();
    }

    @Override
    protected TableLoader getTableLoader() {
        return new GenericTableLoader(configLoader, getDataSourceMetadataService());
    }

    @Override
    protected DruidDimensionsLoader buildDruidDimensionsLoader(
            DruidWebService webService,
            PhysicalTableDictionary physicalTableDictionary,
            DimensionDictionary dimensionDictionary
    ) {
        List<String> dimensionsList = getDimensionConfigurations().stream()
                .map(DimensionConfig::getApiName)
                .collect(Collectors.toList());

        return new DruidDimensionsLoader(
                physicalTableDictionary,
                dimensionDictionary,
                dimensionsList,
                webService
        );
    }

    @Override
    protected MetricLoader getMetricLoader() {
        return new GenericMetricLoader(configLoader);
    }
}
