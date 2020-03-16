// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.keyvaluestore;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.Factory;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStore;

/**
 * A factory that is used by default to support KeyValueStore.
 */
public class MapKeyValueStoreFactory implements Factory<KeyValueStore> {

    /**
     * Build a MapKeyValueStore instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  LuthierConfigNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public KeyValueStore build(String name, LuthierConfigNode configTable, LuthierIndustrialPark resourceFactories) {
        return new MapStore();
    }
}
