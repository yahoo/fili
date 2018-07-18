// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService;
import com.yahoo.luthier.webservice.data.config.ExternalConfigLoader;
import com.yahoo.luthier.webservice.data.config.dimension.ExternalDimensionsLoader;
import com.yahoo.luthier.webservice.data.config.metric.MetricsLoader;
import com.yahoo.luthier.webservice.data.config.table.TablesLoader;

import java.util.LinkedHashSet;

/**
 * TestBinder with Wiki configuration specialization.
 */
public class WikiCopyJerseyTestBinder extends JerseyTestBinder {

    private final String externalConfigFilePath  = System.getProperty("user.dir") + "/config/";

    private static ExternalConfigLoader externalConfigLoader = new ExternalConfigLoader(new ObjectMapper());

    /**
     * Constructor.
     *
     * @param resourceClasses  Resource classes to load into the application.
     */
    public WikiCopyJerseyTestBinder(Class<?>... resourceClasses) {
        this(true, resourceClasses);
    }

    /**
     * Constructor.
     *
     * @param doStart  whether or not to start the application
     * @param resourceClasses Resource classes to load into the application.
     */
    public WikiCopyJerseyTestBinder(boolean doStart, Class<?>... resourceClasses) {
        super(doStart, resourceClasses);
    }

    @Override
    public LinkedHashSet<DimensionConfig> getDimensionConfiguration() {
        return new LinkedHashSet<>(new ExternalDimensionsLoader(
                externalConfigLoader,
                externalConfigFilePath
        ).getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {
        return new MetricsLoader(
                externalConfigLoader,
                externalConfigFilePath
        );
    }

    @Override
    public TableLoader getTableLoader() {
        TablesLoader tablesLoader = new TablesLoader(new TestDataSourceMetadataService());
        tablesLoader.setUp(externalConfigLoader,
                externalConfigFilePath);
        return tablesLoader;
    }
}
