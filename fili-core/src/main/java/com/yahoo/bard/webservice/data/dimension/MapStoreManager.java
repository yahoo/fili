// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import java.util.HashMap;
import java.util.Map;

/**
 * Map Store instance manager.
 */
public class MapStoreManager {

    // Hold singleton instances by name
    private static final Map<String, MapStore> MAP_STORES = new HashMap<>();

    /**
     * Factory for singleton instances by name.
     * <p>
     * Only a single instance can exist for each name.
     *
     * @param storeName Name for the singleton instance
     *
     * @return The singleton instance for the given name
     */
    public static synchronized MapStore getInstance(String storeName) {
        MapStore mapStore = MAP_STORES.get(storeName);

        if (mapStore == null) {
            mapStore = new MapStore();
            MAP_STORES.put(storeName, mapStore);
        }

        return mapStore;
    }

    /**
     * Delete the named singleton instance.
     *
     * @param storeName Name of the singleton instance to delete
     */
    public static synchronized void removeInstance(String storeName) {
        MAP_STORES.remove(storeName);
    }
}
