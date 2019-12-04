// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.searchprovider;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;

/**
 * A factory that is used to support Lucene Search Provider.
 */
public class LuceneSearchProviderFactory implements Factory<SearchProvider> {

    private static final String ENTITY_TYPE = "SearchProvider";
    private static final String INDEX_PATH = "indexPath";
    private static final String MAX_RESULTS = "maxResults";
    private static final String SEARCH_TIMEOUT = "searchTimeout";

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
    public SearchProvider build(String name, LuthierConfigNode configTable, LuthierIndustrialPark resourceFactories) {

        LuthierValidationUtils.validateFields(
                configTable,
                ENTITY_TYPE,
                name,
                INDEX_PATH, MAX_RESULTS, SEARCH_TIMEOUT
        );

        String indexPath  = configTable.get(INDEX_PATH).textValue();
        int maxResults = configTable.get(MAX_RESULTS).intValue();
        int searchTimeout = configTable.get(SEARCH_TIMEOUT).intValue();

        return new LuceneSearchProvider(indexPath, maxResults, searchTimeout);
    }
}
