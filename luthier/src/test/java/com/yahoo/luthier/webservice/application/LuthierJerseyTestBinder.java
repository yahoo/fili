// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.application;

import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import com.yahoo.luthier.webservice.data.config.metric.ExternalMetricsLoader;
import com.yahoo.luthier.webservice.data.config.table.ExternalTableLoader;

import java.util.LinkedHashSet;

/**
 * TestBinder with Wiki configuration specialization.
 */
public class LuthierJerseyTestBinder extends JerseyTestBinder {

    private static final String EXTERNAL_CONFIG_FILE_PATH = "src/test/resources/";
    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader();
    private ExternalDimensionsLoader externalDimensionsLoader;

    /**
     * Constructor.
     *
     * @param resourceClasses  Resource classes to load into the application.
     */
    public LuthierJerseyTestBinder(Class<?>... resourceClasses) {
        this(true, resourceClasses);
    }

    /**
     * Constructor.
     *
     * @param doStart  whether or not to start the application
     * @param resourceClasses Resource classes to load into the application.
     */
    public LuthierJerseyTestBinder(boolean doStart, Class<?>... resourceClasses) {
        super(doStart, resourceClasses);
    }

    @Override
    public LinkedHashSet<DimensionConfig> getDimensionConfiguration() {
        this.externalDimensionsLoader = new ExternalDimensionsLoader(
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
        return new LinkedHashSet<>(externalDimensionsLoader.getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {
        return new ExternalMetricsLoader(
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
    }

    @Override
    public com.yahoo.bard.webservice.data.config.table.TableLoader getTableLoader() {
        return new ExternalTableLoader(
                new TestDataSourceMetadataService(),
                externalDimensionsLoader,
                externalConfigLoader,
                EXTERNAL_CONFIG_FILE_PATH
        );
    }
}
