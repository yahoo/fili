// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.web.util.QueryWeightUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * NoOpSearchProvider factory.
 */
public class NoOpSearchProviderManager {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static Map<String, NoOpSearchProvider> noOpProviders = new HashMap<>();

    private static final int DEFAULT_QUERY_WEIGHT_LIMIT = 100000;

    /**
     * Get instance pointing to a search provider. This method makes sure that there just one instance of search
     * provider for a given dimension.
     *
     * @param providerName name unique identifier for search provider instances
     *
     * @return The search provider instance
     */
    public static synchronized SearchProvider getInstance(String providerName) {
        NoOpSearchProvider noOpProvider = noOpProviders.get(providerName);

        if (noOpProvider == null) {
            noOpProvider = new NoOpSearchProvider(
                    SYSTEM_CONFIG.getIntProperty(QueryWeightUtil.QUERY_WEIGHT_LIMIT_KEY, DEFAULT_QUERY_WEIGHT_LIMIT)
            );
            noOpProviders.put(providerName, noOpProvider);
        }

        return noOpProvider;
    }

    /**
     * Cleanup the existing instance.
     *
     * @param providerName the search provider name
     */
    public static synchronized void removeInstance(String providerName) {
        noOpProviders.remove(providerName);
    }
}
