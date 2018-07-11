// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.DimensionsLoader;
import com.yahoo.luthier.webservice.data.config.metric.MetricsLoader;
import com.yahoo.luthier.webservice.data.config.table.TablesLoader;

import java.util.LinkedHashSet;

/**
 * Wiki specialization of the Abstract Binder factory, applying Wiki configuration objects.
 */
public class WikiCopyBinderFactory extends AbstractBinderFactory {

    private final String externalConfigFilePath  = System.getProperty("user.dir") + "/config/";

    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader(new ObjectMapper());

    @Override
    protected MetricLoader getMetricLoader() {
        return new MetricsLoader(
                externalConfigLoader,
                externalConfigFilePath
        );
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        return new LinkedHashSet<>(
                new DimensionsLoader(
                        externalConfigLoader,
                        externalConfigFilePath
                ).getAllDimensionConfigurations()
        );
    }

    @Override
    protected TableLoader getTableLoader() {
        TablesLoader tablesLoader = new TablesLoader(getDataSourceMetadataService());
        tablesLoader.setUp(externalConfigLoader,
                externalConfigFilePath
        );
        return tablesLoader;
    }
}
