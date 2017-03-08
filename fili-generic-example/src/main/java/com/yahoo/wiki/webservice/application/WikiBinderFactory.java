// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.druid.client.DruidServiceConfig;
import com.yahoo.bard.webservice.druid.client.DruidWebService;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashSet;

/**
 * Wiki specialization of the Abstract Binder factory, applying Wiki configuration objects.
 */
public class WikiBinderFactory extends AbstractBinderFactory {
    private DruidWebService druidWebService;

    @Override
    protected MetricLoader getMetricLoader() {
        return GenericBinder.getInstance(druidWebService).buildMetricLoader();
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        return GenericBinder.getInstance(druidWebService).buildDimensionConfigurations();
    }

    @Override
    protected TableLoader getTableLoader() {
        return GenericBinder.getInstance(druidWebService).buildTableLoader();
    }

    @Override
    protected DruidWebService buildDruidWebService(
            final DruidServiceConfig druidServiceConfig, final ObjectMapper mapper
    ) {
        druidWebService = super.buildDruidWebService(druidServiceConfig, mapper);
        return druidWebService;
    }
}
