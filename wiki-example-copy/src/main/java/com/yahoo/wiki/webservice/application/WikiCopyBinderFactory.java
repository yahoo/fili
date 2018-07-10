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

    String path = System.getProperty("user.dir");

    private final String dimensionExternalConfigFilePath  = path + "/config/DimensionConfigTemplate.json";
    private final String metricExternalConfigFilePath  =  path + "/config/MetricConfigTemplate.json";
    private final String tableExternalConfigFilePath  =  path + "/config/TableConfigTemplate.json";

    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader(new ObjectMapper());

    @Override
    protected MetricLoader getMetricLoader() {
        return new MetricsLoader(
                externalConfigLoader,
                metricExternalConfigFilePath
        );
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        return new LinkedHashSet<>(
                new DimensionsLoader(
                        externalConfigLoader,
                        dimensionExternalConfigFilePath
                ).getAllDimensionConfigurations()
        );
    }

    @Override
    protected TableLoader getTableLoader() {
        TablesLoader tablesLoader = new TablesLoader(getDataSourceMetadataService());
        tablesLoader.setUp(externalConfigLoader,
                tableExternalConfigFilePath
        );
        return tablesLoader;
    }
}
