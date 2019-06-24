// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;

/**
 * A factory that is used by default to support KeyValueStore Dimensions.
 */
public class LuceneSearchProviderFactory implements Factory<SearchProvider> {

    public static final String SEARCH_PROVIDER = "SearchProvider";

    /**
     * Build a SearchProvider instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public SearchProvider build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {

        LuthierValidationUtils.validateField(configTable.get("indexPath"), SEARCH_PROVIDER, name, "indexPath");
        String indexPath  = configTable.get("indexPath").textValue();

        LuthierValidationUtils.validateField(configTable.get("maxResults"), SEARCH_PROVIDER, name, "maxResults");
        int maxResults = configTable.get("maxResults").intValue();

        LuthierValidationUtils.validateField(configTable.get("searchTimeout"), SEARCH_PROVIDER, name, "searchTimeout");
        int searchTimeout = configTable.get("searchTimeout").intValue();

        return new LuceneSearchProvider(indexPath, maxResults, searchTimeout);
    }
}
