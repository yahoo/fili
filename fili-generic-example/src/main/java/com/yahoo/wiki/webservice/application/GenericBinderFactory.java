// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.application.DruidDimensionsLoader;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensions;
import com.yahoo.wiki.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.GenericTableLoader;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Builds dimensions, metrics, and tables for all datasources found from druid.
 */
public class GenericBinderFactory extends AbstractBinderFactory {
    private Supplier<List<? extends DataSourceConfiguration>> configLoader;
    private GenericDimensions genericDimensions;

    @Override
    protected Set<DimensionConfig> getDimensionConfigurations() {
        //NOTE: This is guaranteed to be called before getTableLoader()
        genericDimensions = new GenericDimensions(configLoader);
        return genericDimensions.getAllDimensionConfigurations();
    }

    @Override
    protected TableLoader getTableLoader() {
        return new GenericTableLoader(configLoader, genericDimensions, getDataSourceMetadataService());
    }

    @Override
    protected DruidDimensionsLoader buildDruidDimensionsLoader(
            final DruidWebService webService,
            final PhysicalTableDictionary physicalTableDictionary,
            final DimensionDictionary dimensionDictionary
    ) {
        List<List<Dimension>> dimensions = genericDimensions.getAllDimensionConfigurations().stream()
                .map(DimensionConfig::getApiName)
                .map(dimensionDictionary::findByApiName)
                .map(Collections::singletonList)
                .collect(Collectors.toList());
        return new DruidDimensionsLoader(webService, dimensions, DruidDimensionsLoader.buildDataSourcesList(physicalTableDictionary));
    }

    @Override
    protected MetricLoader getMetricLoader() {
        return new GenericMetricLoader(configLoader);
    }

    @Override
    protected DruidWebService buildDruidWebService(
            DruidServiceConfig druidServiceConfig,
            ObjectMapper mapper
    ) {
        DruidWebService druidWebService = super.buildDruidWebService(druidServiceConfig, mapper);
        configLoader = new DruidNavigator(druidWebService);
        //TODO should probably build my own webservice to be safe but this will work for now
        return druidWebService;
    }
}
