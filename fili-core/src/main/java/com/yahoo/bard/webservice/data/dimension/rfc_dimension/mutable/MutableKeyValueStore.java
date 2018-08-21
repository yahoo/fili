// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.mutable;

import com.yahoo.bard.webservice.data.dimension.rfc_dimension.KeyValueStore;

import java.util.Map;

import javax.validation.constraints.NotNull;

public interface MutableKeyValueStore<K, V> extends KeyValueStore<K, V>
{

    /**
     * Set a value for the key.
     * <p>
     * Also removes the key if the value is null.
     *
     * @param key  Key to set
     * @param value  Value to set for the given key
     *
     * @return The previous value for the key, or null if the key was not set
     */
    String put(@NotNull K key, V value);

    /**
     * Set a multiple key/value pairs.
     * <p>
     * Removes any keys with a null value.
     *
     * @param entries  Map of key/value pairs to put
     *
     * @return The previous values for the keys
     */
    Map<K, V> putAll(@NotNull Map<K, V> entries);

    /**
     * Remove a key from store.
     *
     * @param key  Key to remove
     *
     * @return The previous value for the key, or null if the key was not set
     */
    V remove(@NotNull K key);

}
