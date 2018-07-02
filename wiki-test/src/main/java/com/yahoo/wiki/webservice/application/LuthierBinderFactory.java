// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.wiki.webservice.data.config.ExternalConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.DimensionsLoader;
import com.yahoo.wiki.webservice.data.config.metric.MetricsLoader;
import com.yahoo.wiki.webservice.data.config.table.TablesLoader;

import java.util.LinkedHashSet;

/**
 * Wiki specialization of the Abstract Binder factory, applying Wiki configuration objects.
 */
public class LuthierBinderFactory extends AbstractBinderFactory {


    private static final String DIMENSION_EXTERNAL_CONFIG_FILE_PATH  = "DimensionConfigTemplateSample.json";
    private static final String METRIC_EXTERNAL_CONFIG_FILE_PATH  =  "MetricConfigTemplateSample.json";
    private static final String TABLE_EXTERNAL_CONFIG_FILE_PATH  =  "TableConfigTemplateSample.json";

    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader(new ObjectMapper());

    @Override
    protected MetricLoader getMetricLoader() {
        return new MetricsLoader(
                externalConfigLoader,
                METRIC_EXTERNAL_CONFIG_FILE_PATH
                );
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        return new LinkedHashSet<>(
                new DimensionsLoader(
                        externalConfigLoader,
                        DIMENSION_EXTERNAL_CONFIG_FILE_PATH
                ).getAllDimensionConfigurations()
        );
    }

    @Override
    protected com.yahoo.bard.webservice.data.config.table.TableLoader getTableLoader() {
        TablesLoader tablesLoader = new TablesLoader(getDataSourceMetadataService());
        tablesLoader.setUp(externalConfigLoader,
                TABLE_EXTERNAL_CONFIG_FILE_PATH);
        return tablesLoader;
    }
}
