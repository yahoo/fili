// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A factory that is used by default to support KeyValueStore Dimensions.
 */
public class LuceneSearchProviderFactory implements Factory<SearchProvider> {

    private static final String ENTITY_TYPE = "SearchProvider";

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

        LuthierValidationUtils.validateField(configTable.get("indexPath"), ENTITY_TYPE, name, "indexPath");
        String indexPath  = configTable.get("indexPath").textValue();

        LuthierValidationUtils.validateField(configTable.get("maxResults"), ENTITY_TYPE, name, "maxResults");
        int maxResults = configTable.get("maxResults").intValue();

        LuthierValidationUtils.validateField(configTable.get("searchTimeout"), ENTITY_TYPE, name, "searchTimeout");
        int searchTimeout = configTable.get("searchTimeout").intValue();

        return new LuceneSearchProvider(indexPath, maxResults, searchTimeout);
    }
}
