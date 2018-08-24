// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricsLoader;
import com.yahoo.luthier.webservice.data.config.table.ExternalTableLoader;

import java.util.LinkedHashSet;

/**
 * Wiki specialization of the Abstract Binder factory, applying Wiki configuration objects.
 */
public class WikiCopyBinderFactory extends AbstractBinderFactory {

    private static final String EXTERNAL_CONFIG_FILE_PATH  = System.getProperty("user.dir") + "/config/external/";
    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader();
    private ExternalDimensionsLoader externalDimensionsLoader;

    @Override
    protected MetricLoader getMetricLoader() {
        return new ExternalMetricsLoader(
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        this.externalDimensionsLoader = new ExternalDimensionsLoader(
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
        return new LinkedHashSet<>(externalDimensionsLoader.getAllDimensionConfigurations());
    }

    @Override
    protected TableLoader getTableLoader() {
        return new ExternalTableLoader(
                getDataSourceMetadataService(),
                externalDimensionsLoader,
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
    }
}
