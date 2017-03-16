// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.DruidWebService;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.auto.DruidNavigator;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensions;
import com.yahoo.wiki.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.GenericTableLoader;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;

/**
 * Wiki specialization of the Abstract Binder factory, applying Wiki configuration objects.
 */
public class GenericBinderFactory extends AbstractBinderFactory {
    private static Supplier<List<? extends DataSourceConfiguration>> configLoader;
    private GenericDimensions genericDimensions;

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        //NOTE: This is guaranteed to be called before getTableLoader()
        genericDimensions = new GenericDimensions(configLoader);
        return new LinkedHashSet<>(genericDimensions.getAllDimensionConfigurations());
    }

    @Override
    protected TableLoader getTableLoader() {
        return new GenericTableLoader(configLoader, genericDimensions);
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
        return druidWebService;
    }
}
