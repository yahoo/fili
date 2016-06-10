// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.util.Utils;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Lucene search provider factory
 */

public class LuceneSearchProviderManager {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final @NotNull String LUCENE_INDEX_PATH = SYSTEM_CONFIG.getPackageVariableName("lucene_index_path");
    private static final String HITS_PER_PAGE_PKG_NAME = SYSTEM_CONFIG.getPackageVariableName("lucene_hits_per_page");
    private static final int DEFAULT_HITS_PER_PAGE = 1000000;
    private static final int DEFAULT_MAX_RESULTS_WITHOUT_FILTERS = 10000;
    private static final int MAX_RESULTS_WITHOUT_FILTER = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("max_results_without_filters"),
            DEFAULT_MAX_RESULTS_WITHOUT_FILTERS
    );
    private static final int HITS_PER_PAGE = SYSTEM_CONFIG.getIntProperty(
            HITS_PER_PAGE_PKG_NAME,
            DEFAULT_HITS_PER_PAGE
    );

    private static final Map<String, LuceneSearchProvider> LUCENE_SEARCH_PROVIDERS = new HashMap<>();

    /**
     * Get instance pointing to a search provider This method makes sure that there just one instance of search provider
     * for a given dimension
     *
     * @param providerName name unique identifier for search provider instances
     *
     * @return The lucene search provider
     */
    public static synchronized LuceneSearchProvider getInstance(String providerName) {
        LuceneSearchProvider luceneProvider = LUCENE_SEARCH_PROVIDERS.get(providerName);

        if (luceneProvider == null) {
            luceneProvider = new LuceneSearchProvider(
                    getProviderPath(providerName),
                    HITS_PER_PAGE,
                    MAX_RESULTS_WITHOUT_FILTER
            );
            LUCENE_SEARCH_PROVIDERS.put(providerName, luceneProvider);
        }

        return luceneProvider;
    }

    /**
     * Cleanup the existing instance
     *
     * @param providerName The name of the provider
     */
    public static synchronized void removeInstance(String providerName) {
        LUCENE_SEARCH_PROVIDERS.remove(providerName);
        Utils.deleteFiles(getProviderPath(providerName));
    }

    private static String getProviderPath(String providerName) {
        // Path eg: /home/y/var/bard_webservice/dimension1/lucene_indexes/
        return String.format(
                "%s/dimensionCache/%s/lucene_indexes/",
                SYSTEM_CONFIG.getStringProperty(LUCENE_INDEX_PATH),
                providerName
        );
    }
}
