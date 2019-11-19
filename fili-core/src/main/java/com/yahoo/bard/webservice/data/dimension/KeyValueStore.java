// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * A simple key-value store.
 */
public interface KeyValueStore extends Closeable {

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
     * Remove a key from store.
     *
     * @param key  Key to remove
     *
     * @return The previous value for the key, or null if the key was not set
     */
    String remove(@NotNull String key);

    /**
     * Get the value for a key from store.
     *
     * @param key  Key to get the value for
     *
     * @return the value for corresponding key
     */
    String get(@NotNull String key);

    /**
     * Get the value for a key from store or provide a default.
     *
     * @param key  Key to get the value for
     * @param defaultValue A default value in case key is null
     *
     * @return the value for corresponding key
     */
    default String getOrDefault(@NotNull String key, String defaultValue) {
        return get(key) == null ? defaultValue : get(key);
    }

    /**
     * Get the health status of the store.
     *
     * @return true if store is healthy
     */
    boolean isHealthy();

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
    String put(@NotNull String key, String value);

    /**
     * Set a multiple key/value pairs.
     * <p>
     * Removes any keys with a null value.
     *
     * @param entries  Map of key/value pairs to put
     *
     * @return The previous values for the keys
     */
    Map<String, String> putAll(@NotNull Map<String, String> entries);

    /**
     * Replaces key value store with a new key value store.
     *
     * @param newStorePath  The location of the new store.
     */
    default void replaceStore(String newStorePath) {
        String message = String.format(
                "Current implementation of KeyValueStore: %s does not support replacement operation.",
                this.getClass().getSimpleName()
        );
        LoggerFactory.getLogger(KeyValueStore.class).error(message);
        throw new UnsupportedOperationException(message);
    }
}
