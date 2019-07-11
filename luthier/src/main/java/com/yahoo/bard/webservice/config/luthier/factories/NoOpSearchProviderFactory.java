// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.config.luthier.LuthierValidationUtils;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.NoOpSearchProvider;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A factory that is used by default to support KeyValueStore Dimensions.
 */
public class NoOpSearchProviderFactory implements Factory<SearchProvider> {

    public static final String ENTITY_TYPE = "SearchProvider";

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

        LuthierValidationUtils.validateField(
                configTable.get("queryWeightLimit"),
                ENTITY_TYPE,
                name,
                "queryWeightLimit"
        );
        int queryWeightLimit = configTable.get("queryWeightLimit").intValue();

        return new NoOpSearchProvider(queryWeightLimit);
    }
}
