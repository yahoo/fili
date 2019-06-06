// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application;

import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.ConfigurationLoader;
import com.yahoo.bard.webservice.data.config.LuthierResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.metric.MetricLoader;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.table.TableLoader;
import com.yahoo.bard.webservice.data.dimension.Dimension;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * Extend the abstract binder factory to add external configuration.
 */
public class LuthierBinderFactory extends AbstractBinderFactory {

    @Override
    protected ConfigurationLoader getConfigurationLoader() {
        LuthierResourceDictionaries resourceDictionaries = new LuthierResourceDictionaries();
        initializeDictionaries(resourceDictionaries);
        return new LuthierIndustrialPark.Builder(resourceDictionaries)
                .withDimensionFactories(getDimensionFactories()).build();
    }

    /**
     * Extension point to add default dimension factories.
     *
     * @return  Initializing dimension factories.
     */
    protected Map<String, Factory<Dimension>> getDimensionFactories() {
        return Collections.EMPTY_MAP;
    }

    /**
     * Extension point to add default maker factories.
     *
     * @return  Initializing dimension factories.
     */
    protected Map<String, Factory<MetricMaker>> getMakerFactories() {
        return Collections.EMPTY_MAP;
    }

    /**
     * Extension point to initialize resources into the resource dictionaries.
     *
     * @param resourceDictionaries  The resource dictionaries to prefill.
     */
    protected void initializeDictionaries(LuthierResourceDictionaries resourceDictionaries) {
        resourceDictionaries.getMetricMakerDictionary().putAll(LuthierResourceDictionaries.defaultMakerDictionary());
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
