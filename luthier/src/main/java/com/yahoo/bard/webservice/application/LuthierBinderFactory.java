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
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

/**
 * Extend the abstract binder factory to add external configuration.
 */
public class LuthierBinderFactory extends AbstractBinderFactory {

    @Override
    protected ConfigurationLoader getConfigurationLoader() {
        LuthierResourceDictionaries resourceDictionaries = new LuthierResourceDictionaries();
        initializeDictionaries(resourceDictionaries);
        LuthierIndustrialPark.Builder builder = new LuthierIndustrialPark.Builder(resourceDictionaries);
        getDimensionFactories().ifPresent(builder::withDimensionFactories);
        getSearchProviderFactories().ifPresent(builder::withSearchProviderFactories);
        getKeyValueStoreFactories().ifPresent(builder::withKeyValueStoreFactories);
        return builder.build();
    }

    /**
     * Extension point to add default dimension factories.
     * If it does not return Optional.empty, overwrites the factories specified in LuthierIndustrialPark.
     *
     * @return  Optional default Dimension factories.
     */
    protected Optional<Map<String, Factory<Dimension>>> getDimensionFactories() {
        return Optional.empty();
    }


    /**
     * Extension point to add default searchProvider factories.
     * If it does not return Optional.empty, overwrites the factories specified in LuthierIndustrialPark.
     *
     * @return  Optional default SearchProvider factories.
     */
    protected Optional<Map<String, Factory<SearchProvider>>> getSearchProviderFactories() {
        return Optional.empty();
    }

    /**
     * Temp stuff.
     * @return  Optional.empty()
     */
    protected Optional<Map<String, Factory<KeyValueStore>>> getKeyValueStoreFactories() {
        return Optional.empty();
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
        resourceDictionaries.getSearchProviderDictionary().putAll(
                LuthierResourceDictionaries.defaultSearchProviderDictionary()
        );
        resourceDictionaries.getKeyValueStoreDictionary().putAll(
                LuthierResourceDictionaries.defaultKeyValueStoreDictionary()
        );
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
