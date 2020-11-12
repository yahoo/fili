// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of the tuple data cache using a HashMap. Uses plain text keys instead of their hashes.
 */
public class TestTupleDataCache implements TupleDataCache<String, Long, String> {
    final Map<String, DataEntry<String, Long, String>> client = new ConcurrentHashMap<>();

    @Override
    public TupleDataCache.DataEntry<String, Long, String> get(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalStateException("Invalid key: " + key);
        }
        return client.get(key);
    }

    @Override
    public boolean set(String key, TupleDataCache.DataEntry<String, Long, String> value)
            throws IllegalStateException {
        client.put(key, value);
        return true;
    }

    @Override
    public String getDataValue(String key) {
        TupleDataCache.DataEntry<String, Long, String> result = get(key);
        return result != null ? result.getValue() : null;
    }

    //(Deprecate this return type to be void)
    @Override
    public boolean set(String key, Long meta, String value) throws IllegalStateException {
        return set(key, new MemTupleDataCache.DataEntry<>(key, meta, value));
    }

    @Override
    public void clear() {
        client.clear();
    }
}
