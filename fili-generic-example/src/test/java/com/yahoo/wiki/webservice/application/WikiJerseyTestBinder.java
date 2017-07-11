// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.auto.StaticWikiConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensionConfigs;
import com.yahoo.wiki.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.GenericTableLoader;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;

/**
 * TestBinder with Wiki configuration specialization.
 */
public class WikiJerseyTestBinder extends JerseyTestBinder {
    private static Supplier<List<? extends DataSourceConfiguration>> configLoader = new StaticWikiConfigLoader();
    private GenericDimensionConfigs genericDimensionConfigs;

    /**
     * Constructor.
     *
     * @param resourceClasses  Resource classes to load into the application.
     */
    public WikiJerseyTestBinder(java.lang.Class<?>... resourceClasses) {
        this(true, resourceClasses);
    }

    /**
     * Constructor.
     *
     * @param doStart  Whether or not to start the application
     * @param resourceClasses  Resource classes to load into the application.
     */
    public WikiJerseyTestBinder(boolean doStart, java.lang.Class<?>... resourceClasses) {
        super(doStart, resourceClasses);
    }

    @Override
    public LinkedHashSet<DimensionConfig> getDimensionConfiguration() {
        genericDimensionConfigs = new GenericDimensionConfigs(configLoader);
        return new LinkedHashSet<>(genericDimensionConfigs.getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {
        return new GenericMetricLoader(configLoader);
    }

    @Override
    public TableLoader getTableLoader() {
        return new GenericTableLoader(configLoader, genericDimensionConfigs, new DataSourceMetadataService());
    }
}
