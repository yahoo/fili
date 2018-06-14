// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.JerseyTestBinder;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService;
import com.yahoo.wiki.webservice.data.config.dimension.WikiDimensions;
import com.yahoo.wiki.webservice.data.config.metric.WikiMetricLoader;
import com.yahoo.wiki.webservice.data.config.table.WikiTableLoader;

import java.util.LinkedHashSet;

/**
 * TestBinder with Wiki configuration specialization.
 */
public class WikiJerseyTestBinder extends JerseyTestBinder {

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
        return new LinkedHashSet<>(new WikiDimensions().getAllDimensionConfigurations());
    }

    @Override
    public MetricLoader getMetricLoader() {
        return new WikiMetricLoader();
    }

    @Override
    public TableLoader getTableLoader() {
        return new WikiTableLoader(new TestDataSourceMetadataService());
    }
}
