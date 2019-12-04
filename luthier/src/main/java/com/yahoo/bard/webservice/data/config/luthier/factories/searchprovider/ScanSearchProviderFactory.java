// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.searchprovider;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProvider;

/**
 * A factory that is used by default to support KeyValueStore Dimensions.
 */
public class ScanSearchProviderFactory implements Factory<SearchProvider> {

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
        return new ScanSearchProvider();
    }
}
