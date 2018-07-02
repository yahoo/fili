// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService;
import com.yahoo.wiki.webservice.data.config.ExternalConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.DimensionsLoader;
import com.yahoo.wiki.webservice.data.config.metric.MetricsLoader;
import com.yahoo.wiki.webservice.data.config.table.TablesLoader;

import java.util.LinkedHashSet;

/**
 * TestBinder with Wiki configuration specialization.
 */
public class LuthierJerseyTestBinder extends JerseyTestBinder {

    private static final String DIMENSION_EXTERNAL_CONFIG_FILE_PATH  = "DimensionConfigTemplateSample.json";
    private static final String METRIC_EXTERNAL_CONFIG_FILE_PATH  =  "MetricConfigTemplateSample.json";
    private static final String TABLE_EXTERNAL_CONFIG_FILE_PATH  =  "TableConfigTemplateSample.json";

    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader(new ObjectMapper());

    /**
     * Constructor.
     *
     * @param resourceClasses  Resource classes to load into the application.
     */
    public LuthierJerseyTestBinder(java.lang.Class<?>... resourceClasses) {
        this(true, resourceClasses);
    }

    /**
     * Constructor.
     *
     * @param doStart  whether or not to start the application
     * @param resourceClasses Resource classes to load into the application.
     */
    public LuthierJerseyTestBinder(boolean doStart, java.lang.Class<?>... resourceClasses) {
        super(doStart, resourceClasses);
    }

    @Override
    public LinkedHashSet<DimensionConfig> getDimensionConfiguration() {
        return new LinkedHashSet<>(new DimensionsLoader(
                externalConfigLoader,
                DIMENSION_EXTERNAL_CONFIG_FILE_PATH
        ).getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {
        return new MetricsLoader(
                externalConfigLoader,
                METRIC_EXTERNAL_CONFIG_FILE_PATH
        );
    }

    @Override
    public com.yahoo.bard.webservice.data.config.table.TableLoader getTableLoader() {
        TablesLoader tablesLoader = new TablesLoader(new TestDataSourceMetadataService());
        tablesLoader.setUp(externalConfigLoader,
                TABLE_EXTERNAL_CONFIG_FILE_PATH);
        return tablesLoader;
    }
}
