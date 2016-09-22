// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * A HashMap-based implementation of KeyValueStore.
 */
public class MapStore implements KeyValueStore {

    // The actual key/value store
    private final Map<String, String> store;

    /**
     * Private constructor for the singleton pattern.
     */
    public MapStore() {
        store = new HashMap<>();
    }

    @Override
    public void open() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public boolean isOpen() {
        // An in-memory map is always open
        return true;
    }

    @Override
    public String remove(@NotNull String key) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot remove null key");
        }

        return store.remove(key);
    }

    @Override
    public String get(@NotNull String key) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot get null key");
        }

        return store.get(key);
    }

    @Override
    public boolean isHealthy() {
        // An in-memory map is always healthy
        return true;
    }

    @Override
    public String put(@NotNull String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Cannot set null key");
        }

        if (value == null) {
            return store.remove(key);
        } else {
            return store.put(key, value);
        }
    }

    @Override
    public Map<String, String> putAll(@NotNull Map<String, String> entries) {
        if (entries == null) {
            throw new IllegalArgumentException("Cannot set null entries");
        }

        Map<String, String> oldValues = new HashMap<>(entries.size());
        for (String key : entries.keySet()) {
            String oldValue = put(key, entries.get(key));
            oldValues.put(key, oldValue);
        }
        return oldValues;
    }
}
