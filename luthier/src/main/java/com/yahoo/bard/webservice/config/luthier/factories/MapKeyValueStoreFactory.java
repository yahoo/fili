// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier.factories;

import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.MapStore;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A factory that is used by default to support KeyValueStore.
 */
public class MapKeyValueStoreFactory implements Factory<KeyValueStore> {

    /**
     * Build a MapKeyValueStore instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  ObjectNode that points to the value of corresponding table entry in config file
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public KeyValueStore build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        return new MapStore();
    }
}
