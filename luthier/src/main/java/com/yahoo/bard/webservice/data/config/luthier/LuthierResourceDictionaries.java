// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An extension of resource dictionaries providing for config entities used to supply dependencies.
 */
public class LuthierResourceDictionaries extends ResourceDictionaries {

    private final Map<String, MetricMaker> metricMakerDictionary;

    private final Map<String, SearchProvider> searchProviderDictionary;

    private final Map<String, KeyValueStore> keyValueStoreDictionary;

    /**
     * Constructor.
     */
    public LuthierResourceDictionaries() {
        super();
        metricMakerDictionary = defaultMakerDictionary();
        searchProviderDictionary = defaultSearchProviderDictionary();
        keyValueStoreDictionary = defaultKeyValueStoreDictionary();
    }

    public Map<String, MetricMaker> getMetricMakerDictionary() {
        return metricMakerDictionary;
    }
    public Map<String, SearchProvider> getSearchProviderDictionary() {
        return searchProviderDictionary;
    }
    public Map<String, KeyValueStore> getKeyValueStoreDictionary() {
        return keyValueStoreDictionary;
    }

    /**
     * Supply the default searchProvider dictionaries available in the entire application.
     *
     * @return  A map of named SearchProviders
     */
    public static Map<String, SearchProvider> defaultSearchProviderDictionary() {
        return new LinkedHashMap<>();
    }

    /**
     * Supply the default keyValueStore dictionaries available in the entire application.
     *
     * @return  A map of named KeyValueStores
     */
    public static Map<String, KeyValueStore> defaultKeyValueStoreDictionary() {
        return new LinkedHashMap<>();
    }

    /**
     * Supply the default maker dictionaries available in the entire application.
     *
     * @return  A map of named MetricMakers
     */
    public static Map<String, MetricMaker> defaultMakerDictionary() {
        // add Longsum, doublesum, sketchCount, etc
        // plusMaker, minusMaker
        return new HashMap<>();
    }
}
