// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.config;

import com.yahoo.bard.webservice.data.dimension.SearchProvider;

// Generalizes the LuceneSearchProviderManager, NoOpSearchProviderManager, etc
public interface SearchProviderFactory {
    /**
     * Get instance pointing to a search provider. This method makes sure that there just one instance of search
     * provider for a given dimension.
     *
     * @param providerName  Name unique identifier for search provider instances
     *
     * @return the lucene search provider
     */
    SearchProvider build(String providerName);

    /**
     * Cleanup the existing instance.
     *
     * @param providerName The name of the provider
     */
    void removeInstance(String providerName);
}
