// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import java.util.LinkedHashMap;

/**
 * Hashmap backed data cache.
 */
public class TestDataCache implements DataCache<HashDataCache.Pair<String, String>> {
    LinkedHashMap<String, HashDataCache.Pair<String, String>> cache = new LinkedHashMap<>();

    static boolean cacheEnabled = true;

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public HashDataCache.Pair<String, String> get(String key) throws IllegalStateException {
        if (!cacheEnabled) {
            throw new IllegalStateException("cache disabled");
        }
        if (key == null || key.isEmpty() || key.length() > 100) {
            throw new IllegalStateException();
        }
        return cache.get(key);
    }

    @Override
    public boolean set(String key, HashDataCache.Pair<String, String> value) throws IllegalStateException {
        if (!cacheEnabled) {
            throw new IllegalStateException("cache disabled");
        }
        cache.put(key, value);
        return Boolean.TRUE;
    }
}
