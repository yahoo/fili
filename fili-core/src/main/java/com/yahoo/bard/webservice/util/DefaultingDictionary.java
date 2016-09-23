// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DefaultingDictionary is a Map that returns a default value if a given key is not present in the map, or returns the
 * value associated with the given key.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class DefaultingDictionary<K, V> extends LinkedHashMap<K, V> {

    private final V defaultValue;

    /**
     * Construct the defaultingDictionary using the defaultValue provided.
     *
     * @param defaultValue  The defaultValue to be returned if a key is not present in the map
     */
    public DefaultingDictionary(V defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * Construct DefaultingDictionary using the given defaultValue and entries from the given defaultingDictionaryMap.
     *
     * @param defaultValue  The defaultValue to be returned if a key is not present in the map
     * @param defaultingDictionaryMap  A Map whose entries will be inserted into the DefaultingDictionary
     */
    public DefaultingDictionary(V defaultValue, Map<K, V> defaultingDictionaryMap) {
        this.defaultValue = defaultValue;
        this.putAll(defaultingDictionaryMap);
    }

    /**
     * Return the defaultValue for this class.
     *
     * @return defaultValue for this class
     */
    public V getDefaultValue() {
        return defaultValue;
    }

    @Override
    public V get(Object key) {
        return getOrDefault(key, defaultValue);
    }
}
