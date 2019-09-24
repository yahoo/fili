// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.Objects;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Unmodifiable extension of a LinkedHashSet.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class UnmodifiableLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    private final Map<K, V> target;

    /**
     * Constructor. Build through static factory method {@link UnmodifiableLinkedHashMap#of}.
     *
     * @param target  The underlying map that is being copied.
     */
    private UnmodifiableLinkedHashMap(LinkedHashMap<K, V> target) {
        this.target = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(target)));
    }

    /**
     * Static factory.
     *
     * @param target  Map to provide the view of
     * @param <K>  Key type
     * @param <V>  Value type
     * @return the unmodifiable linked hash map.
     */
    public static <K, V> UnmodifiableLinkedHashMap<K, V> of(LinkedHashMap<K, V> target) {
        return new UnmodifiableLinkedHashMap<>(target);
    }

    @Override public int size() {
        return target.size();
    }
    @Override public boolean isEmpty() {
        return target.isEmpty();
    }
    @Override public boolean containsKey(Object key) {
        return target.containsKey(key);
    }
    @Override public boolean containsValue(Object val) {
        return target.containsValue(val);
    }
    @Override public V get(Object key) {
        return target.get(key);
    }

    @Override public V put(K key, V value) {
        return target.put(key, value);
    }
    @Override public V remove(Object key) {
        return target.remove(key);
    }
    @Override public void putAll(Map<? extends K, ? extends V> m) {
        target.putAll(m);
    }
    @Override public void clear() {
        target.clear();
    }
    @Override public Set<K> keySet() {
        return target.keySet();
    }
    @Override public Set<Map.Entry<K, V>> entrySet() {
        return target.entrySet();
    }
    @Override public Collection<V> values() {
        return target.values();
    }

    @Override public boolean equals(Object o) {
        return target.equals(o);
    }
    @Override public int hashCode() {
        return target.hashCode();
    }
    @Override public String toString() {
        return target.toString();
    }

    @Override @SuppressWarnings("unchecked") public V getOrDefault(Object k, V defaultValue) {
        return target.getOrDefault(k, defaultValue);
    }

    @Override public void forEach(BiConsumer<? super K, ? super V> action) {
        target.forEach(action);
    }

    @Override public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        target.replaceAll(function);
    }

    @Override public V putIfAbsent(K key, V value) {
        return target.putIfAbsent(key, value);
    }

    @Override public boolean remove(Object key, Object value) {
        return target.remove(key, value);
    }

    @Override public boolean replace(K key, V oldValue, V newValue) {
        return target.replace(key, oldValue, newValue);
    }

    @Override public V replace(K key, V value) {
        return target.replace(key, value);
    }

    @Override public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return target.computeIfAbsent(key, mappingFunction);
    }

    @Override public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return target.computeIfPresent(key, remappingFunction);
    }

    @Override public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return target.compute(key, remappingFunction);
    }

    @Override public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return target.merge(key, value, remappingFunction);
    }
}
