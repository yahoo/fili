// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import java.io.Serializable;

/**
 * Versatile data cache interface that allows for parametrized types for the key, the metadata and the raw data value
 * of an entry.
 *
 * @param <K>  The key type of the cache.
 * @param <M>  The metadata type of the cache.
 * @param <V>  The raw data type of the cache.
 */
public interface TupleDataCache<K, M extends Serializable, V extends Serializable>
        extends DataCache<TupleDataCache.DataEntry<K, M, V>> {
    /**
     * Retrieve the complete data entry from the cache.
     *
     * @param key  The key associated with data entry to be retrieved
     *
     * @return The complete data entry containing metadata and raw data, or {@code null} if the cache contains no
     * mapping for the key
     */
    @Override
    DataEntry<K, M, V> get(String key);

    /**
     * Read the raw data from cache.
     *
     * @param key  The key whose associated value is to be returned
     *
     * @return the value to which the specified key is mapped, or {@code null} if this map contains no mapping for
     * the key
     */
    V getDataValue(K key);

    /**
     * Given a key, put a complete data entry in the data cache.
     *
     * @param key  The key of the cache entry.
     * @param meta  The metadata associated with the raw data.
     * @param value  The raw data to store in to the cache.
     *
     * @return true if the entry is stored successfully; false otherwise.
     *
     * @throws IllegalStateException in the rare circumstance where queue is too full to accept any more requests
     */
    boolean set(K key, M meta, V value);

    /**
     * A data cache entry defined as a tuple consisting of key, metadata, and value fields.
     *
     * @param <K>  The key type of the cache.
     * @param <M>  The metadata type of the cache.
     * @param <V>  The raw data type of the cache.
     */
    interface DataEntry<K, M, V> extends Serializable {
        /**
         * Returns the key of this data cache entry.
         *
         * @return the key corresponding to this entry.
         */
        K getKey();

        /**
         * Returns the metadata of this data cache entry.
         *
         * @return the metadata corresponding to this entry.
         */
        M getMeta();

        /**
         * Returns the value of this data cache entry.
         *
         * @return the value corresponding to this entry.
         */
        V getValue();
    }
}
