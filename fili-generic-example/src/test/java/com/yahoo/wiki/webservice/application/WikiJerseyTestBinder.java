// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.wiki.webservice.data.config.auto.ConfigLoader;
import com.yahoo.wiki.webservice.data.config.auto.StaticWikiConfigLoader;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensions;
import com.yahoo.wiki.webservice.data.config.metric.GenericMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.GenericTableLoader;

import java.util.LinkedHashSet;

/**
 * TestBinder with Wiki configuration specialization.
 */
public class WikiJerseyTestBinder extends JerseyTestBinder {
    private static ConfigLoader configLoader = new StaticWikiConfigLoader();
    private GenericDimensions genericDimensions;

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
     * @param doStart  whether or not to start the application
     * @param resourceClasses Resource classes to load into the application.
     */
    public WikiJerseyTestBinder(boolean doStart, java.lang.Class<?>... resourceClasses) {
        super(doStart, resourceClasses);
    }

    @Override
    public LinkedHashSet<DimensionConfig> getDimensionConfiguration() {
        genericDimensions = new GenericDimensions(configLoader);
        return new LinkedHashSet<>(genericDimensions.getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {
        return new GenericMetricLoader(configLoader);
    }

    @Override
    public TableLoader getTableLoader() {
        return new GenericTableLoader(configLoader,genericDimensions);
    }
}
