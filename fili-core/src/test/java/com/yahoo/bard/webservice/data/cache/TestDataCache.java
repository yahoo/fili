// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.joda.time.DateTime;

import java.util.LinkedHashMap;

/**
 * Hashmap backed data cache.
 */
public class TestDataCache implements DataCache<HashDataCache.Pair<String, String>> {
    LinkedHashMap<String, Pair<DateTime, HashDataCache.Pair<String, String>>> cache = new LinkedHashMap<>();

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
        Pair<DateTime, HashDataCache.Pair<String, String>> result = cache.get(key);
        if (result == null) {
            return null;
        }
        DateTime expiration = result.getLeft();
        if (expiration != null && new DateTime().isAfter(expiration)) {
            cache.remove(key);
            return null;
        }
        return result.getRight();
    }

    @Override
    public boolean set(String key, HashDataCache.Pair<String, String> value) throws IllegalStateException {
        return set(key, value, null);
    }

    @Override
    public boolean set(
            String key,
            HashDataCache.Pair<String, String> value, DateTime expiration
    ) throws IllegalStateException {
        if (!cacheEnabled) {
            throw new IllegalStateException("cache disabled");
        }
        cache.put(key, new ImmutablePair<>(expiration, value));
        return Boolean.TRUE;
    }
}
