// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.application;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;

import java.util.LinkedHashSet;

/**
 * Wiki specialization of the Abstract Binder factory, applying Wiki configuration objects.
 */
public class WikiBinderFactory extends AbstractBinderFactory {

    @Override
    protected MetricLoader getMetricLoader() {
        return GenericBinder.getInstance().buildMetricLoader();
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        return GenericBinder.getInstance().buildDimensionConfigurations();
    }

    @Override
    protected TableLoader getTableLoader() {
        return GenericBinder.getInstance().buildTableLoader();
    }
}
