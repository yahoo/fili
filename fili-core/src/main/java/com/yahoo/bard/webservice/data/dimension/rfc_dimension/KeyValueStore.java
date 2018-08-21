// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension;

import java.io.Closeable;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * A simple KeyValueStore interface.
 *
 * Parameterized to support stores which reify into objects or return byte[].
 */
public interface KeyValueStore<K, V> extends /*com.yahoo.bard.webservice.data.dimension.KeyValueStore,*/ Closeable {

    /**
     * Open the store.
     */
    void open();

    /**
     * Close the store.
     */
    void close();

    /**
     * Get the open / closed status of the store.
     *
     * @return true if the store is open
     */
    boolean isOpen();

    /**
     * Get the value for a key from store.
     *
     * @param key  Key to get the value for
     *
     * @return the value for corresponding key
     */
    V get(@NotNull K key);

    /**
     * Get the values for a stream of keys from store.
     *
     * @param key  Key to get the value for
     *
     * @return the value for corresponding key
     */

    default Stream<V> getValues(@NotNull Stream<K> key) {
        return key.map(this::get);
    }

    /**
     * Get the value for a key from store or provide a default.
     *
     * @param key  Key to get the value for
     * @param defaultValue A default value in case key is null
     *
     * @return the value for corresponding key
     */

    default V getOrDefault(@NotNull K key, V defaultValue) {
        return get(key) == null ? defaultValue : get(key);
    }

    /**
     * Get the health status of the store.
     *
     * @return true if store is healthy
     */
    boolean isHealthy();
}
