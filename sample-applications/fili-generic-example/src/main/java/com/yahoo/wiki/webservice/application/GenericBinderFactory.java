// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.application.DimensionValueLoadTask;
import com.yahoo.bard.webservice.application.DruidDimensionValueLoader;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensionConfigs;
import com.yahoo.wiki.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.GenericTableLoader;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Builds dimensions, metrics, and tables for all datasources found from druid.
 */
public class GenericBinderFactory extends AbstractBinderFactory {
    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;
    private final GenericDimensionConfigs genericDimensionConfigs;

    /**
     * Constructs a GenericBinderFactory using the MetadataDruidWebService
     * to configure dimensions, tables, and metrics from Druid.
     */
    public GenericBinderFactory() {
        DruidWebService druidWebService = buildMetadataDruidWebService(getMappers().getMapper());
        configLoader = new DruidNavigator(druidWebService, getMapper());
        genericDimensionConfigs = buildDimensionConfigs();
    }

    /**
     * Method to create the dimension config source for generic dimension loading.
     *
     * @return An instance of GenericDimensionConfigurations (collection wrapper and dimension metadata tools)
     */
    protected GenericDimensionConfigs buildDimensionConfigs() {
        return new GenericDimensionConfigs(configLoader);
    }

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        return genericDimensionConfigs.getAllDimensionConfigurations();
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
        return new GenericMetricLoader(configLoader);
    }
}
