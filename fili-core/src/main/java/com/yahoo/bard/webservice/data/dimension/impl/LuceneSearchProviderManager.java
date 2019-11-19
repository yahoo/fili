// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.impl;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * Lucene search provider factory.
 */
public class LuceneSearchProviderManager {
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final @NotNull String LUCENE_INDEX_PATH = SYSTEM_CONFIG.getPackageVariableName("lucene_index_path");

    private static final Map<String, LuceneSearchProvider> LUCENE_SEARCH_PROVIDERS = new HashMap<>();

    /**
     * Get instance pointing to a search provider. This method makes sure that there just one instance of search
     * provider for a given dimension.
     *
     * @param providerName  Name unique identifier for search provider instances
     *
     * @return the lucene search provider
     */
    public static LuceneSearchProvider getInstance(String providerName) {
        synchronized (LuceneSearchProviderManager.class) {
            LuceneSearchProvider luceneProvider = LUCENE_SEARCH_PROVIDERS.get(providerName);

            if (luceneProvider == null) {
                luceneProvider = new LuceneSearchProvider(
                        getProviderPath(providerName),
                        PaginationParameters.EVERYTHING_IN_ONE_PAGE.getPerPage()
                );
                LUCENE_SEARCH_PROVIDERS.put(providerName, luceneProvider);
            }

            return luceneProvider;
        }
    }

    /**
     * Cleanup the existing instance.
     *
     * @param providerName The name of the provider
     */
    public static void removeInstance(String providerName) {
        synchronized (LuceneSearchProviderManager.class) {
            LUCENE_SEARCH_PROVIDERS.remove(providerName);
            Utils.deleteFiles(getProviderPath(providerName));
        }
    }

    /**
     * Get the path for the lucene index files for a provider.
     *
     * @param providerName  Name of the provider
     *
     * @return the path to the lucene index files for this provider.
     */
    private static String getProviderPath(String providerName) {
        // Path eg: /home/y/var/bard_webservice/dimension1/lucene_indexes/
        return String.format(
                "%s/dimensionCache/%s/lucene_indexes/",
                SYSTEM_CONFIG.getStringProperty(LUCENE_INDEX_PATH),
                providerName
        ).replaceAll("/+", "/"); // replaces one or more slashes with one slash:
    }
}
