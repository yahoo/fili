// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import java.util.HashMap;
import java.util.Map;

/**
 * Scan search provider instances factory.
 */

public class ScanSearchProviderManager {
    private static Map<String, ScanSearchProvider> scanProviders = new HashMap<>();

    /**
     * Get instance pointing to a search provider This method makes sure that there just one instance of search provider
     * for a given dimension.
     *
     * @param providerName name unique identifier for search provider instances
     *
     * @return The search provider instance
     */
    public static synchronized ScanSearchProvider getInstance(String providerName) {
        ScanSearchProvider scanProvider = scanProviders.get(providerName);

        if (scanProvider == null) {
            scanProvider = new ScanSearchProvider();
            scanProviders.put(providerName, scanProvider);
        }

        return scanProvider;
    }

    /**
     * Cleanup the existing instance.
     *
     * @param providerName The name of the provider
     */
    public static synchronized void removeInstance(String providerName) {
        scanProviders.remove(providerName);
    }
}
