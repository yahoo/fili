// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.luthier.LuthierConfigurationLoader;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.table.TableLoader;

import java.util.LinkedHashSet;

/**
 * Wiki specialization of the Abstract Binder factory, applying Wiki configuration objects.
 */
public class LuthierBinderFactory extends AbstractBinderFactory {

    @Override
    protected ConfigurationLoader getConfigurationLoader() {
        LuthierResourceDictionaries resourceDictionaries = new LuthierResourceDictionaries();
        resourceDictionaries.getMetricMakerDictionary().putAll(LuthierResourceDictionaries.defaultMakerDictionary());
        return new LuthierConfigurationLoader(resourceDictionaries);
    }

    @Override
    protected MetricLoader getMetricLoader() {
        return null;
    }

    @Override
    protected LinkedHashSet<DimensionConfig> getDimensionConfigurations() {
        return null;
    }

    @Override
    protected TableLoader getTableLoader() {
        return null;
    }
}
