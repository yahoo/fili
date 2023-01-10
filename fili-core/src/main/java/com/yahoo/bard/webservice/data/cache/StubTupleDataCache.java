// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.cache;

import java.io.Serializable;

/**
 * Memcached client implementation of TupleDataCache.
 *
 * @param <T>  The meta data type of the cache.
 * @param <V>  The raw data type of the cache.
 */
public class StubTupleDataCache<T extends Serializable, V extends Serializable>
        implements TupleDataCache<String, T, V> {

    public StubTupleDataCache() {
    }

    @Override
    public DataEntry<String, T, V> get(final String key) {
        return null;
    }

    @Override
    public V getDataValue(final String key) {
        return null;
    }

    @Override
    public boolean set(final String key, final T meta, final V value) {
        return false;
    }

    @Override
    public boolean set(final String key, final DataEntry<String, T, V> value) throws IllegalStateException {
        return false;
    }

    @Override
    public void clear() {
    }
}
