// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.cache;

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
     * @param expiration The expiration time, it may either be Unix time (number of seconds since January 1, 1970, as
     * a 32-bit value), or a number of seconds starting from current time. In the latter case, this number of seconds
     * may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that,
     * the server will consider it to be real Unix time value rather than an offset from current time.
     *
     * @return a boolean representing success of this operation
     * @throws IllegalStateException in the rare circumstance where queue is too
     * full to accept any more requests
     */
    default boolean set(String key, T value, int expiration) throws IllegalStateException {
        return set(key, value);
    }

    /**
     * Removes all of the mappings from this cache.
     */
    void clear();
}
