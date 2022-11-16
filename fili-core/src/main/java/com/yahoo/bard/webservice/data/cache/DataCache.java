// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

import org.joda.time.DateTime;

import java.io.Serializable;

/**
 * A very thin wrapper around key value storage.
 *
 * @param <T> The value type being stored
 */
public interface DataCache<T extends Serializable> {

    /**
     * Read data from cache.
     *
     * @param key  the key whose associated value is to be returned
     *
     * @return the value to which the specified key is mapped, or {@code null} if this map contains no mapping for the
     * key
     */
    T get(String key);

    /**
     * Put a value on a key in a data cache.
     *
     * @param key  the key under which this object should be added.
     * @param value  the object to store
     *
     * (Deprecate this return type to be void)
     * @return a boolean representing success of this operation
     * @throws IllegalStateException in the rare circumstance where queue is too
     * full to accept any more requests
     */
    boolean set(String key, T value) throws IllegalStateException;

    /**
     * Put a value on a key in a data cache with the specified expiration.
     *
     * By default, this method throws away the expiration.
     *
     *
     * @param key  the key under which this object should be added.
     * @param value  the object to store
     * @param expiration The date on which this key should expire
     *
     *  //(Deprecate this return type to be void)
     * @return a boolean representing success of this operation
     * @throws IllegalStateException in the rare circumstance where queue is too
     * full to accept any more requests
     */
    default boolean set(String key, T value, DateTime expiration) throws IllegalStateException {
        return set(key, value);
    }

    /**
     * Removes all of the mappings from this cache.
     */
    void clear();
}
