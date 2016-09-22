// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * A delegating map has a local copy (self) of keys and values and a next map to which it delegates lookups for values
 * which it doesn't contain. Operations which remove keys from a map throw exceptions because the local map cannot alter
 * its delegate (removing a key with remove(K k) from the map and then still having get(K k) return the value from the
 * delegate would be very surprising behavior on a Map).
 * <p>
 * In order to support predictable iteration ordering, this extends LinkedHashMap with a modified definition of
 * canonical ordering based on the canonical ordering of non-shadowed keys in the delegate followed by local keys in
 * insertion order (e.g. LinkedHashMap).
 *
 * @param <K> The key type for the map
 * @param <V> The value type for the map
 */
public class DelegatingMap<K, V> extends LinkedHashMap<K, V> {

    private final Map<K, V> delegate;

    /**
     * Constructor.
     * <p>
     * Uses a LinkedHashMap as it's base map.
     */
    public DelegatingMap() {
        this(new LinkedHashMap<>());
    }

    /**
     * Constructor.
     *
     * @param nextMap  Map to delegate to
     */
    public DelegatingMap(@NotNull Map<K, V> nextMap) {
        this.delegate = nextMap;
    }

    @Override
    public V get(Object key) {
        return super.containsKey(key) ? super.get(key) : delegate.get(key);
    }

    /**
     * Removes are not allowed: Delegating Map should be changed in a put-only fashion.
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException(
                "Clear is not defined on DelegatingMap. Add is the only allowed modification operation."
        );
    }

    /**
     * Clear the local map. This won't affect the delegate map.
     */
    public void clearLocal() {
        super.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(key) || delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object o) {
        return entryStream().map(Map.Entry::getValue).anyMatch(v -> Objects.equals(v, o));
    }

    @Override
    public Set<K> keySet() {
        return entryStream()
            .map(Map.Entry::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Get a stream of map entries.
     *
     * @return A stream of entries which are visible from via delegate and those in the local map
     */
    private Stream<Map.Entry<K, V>> entryStream() {
        return Stream.concat(
                delegate.entrySet().stream().filter(e -> !super.containsKey(e.getKey())),
                super.entrySet().stream()
        );
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return entryStream().collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Adds the key and value to the local map, potentially overshadowing a version in the delegate map.
     *
     * @param key The key to the value
     * @param value The value being stored
     *
     * @return the prior value from the local map (if any) or the shadowed value from the delegate (if any)
     */
    @Override
    public V put(K key, V value) {
        V oldValue = super.put(key, value);
        return (oldValue == null) ? delegate.get(key) : oldValue;
    }

    /**
     * Delegating map should be changed in a put-only fashion.  Removes are not allowed.
     *
     * @param o  Object to be removed
     *
     * @return nothing, this is not supported and throws an exception instead
     */
    @Override
    public V remove(Object o) {
        throw new UnsupportedOperationException(
                "Remove is not defined on DelegatingMap. Add is the only allowed modification operation."
        );
    }

    /**
     * Remove from the local map.
     *
     * @param key  key whose mapping is to be removed from the map
     *
     * @return the previous value associated with key, or null if there was no mapping for key.
     */
    public V removeLocal(K key) {
        return super.remove(key);
    }

    @Override
    public int size() {
        return entrySet().size();
    }

    @Override
    public Collection<V> values() {
        return entryStream().map(Map.Entry::getValue).collect(Collectors.toSet());
    }

    /**
     * Return a non delegating map which snapshots the data visible in this delegating map, disconnected from changes
     * to this and the underlying delegates.
     *
     * @return A non delegating plain-map copy of the data visible in this map
     */
    public LinkedHashMap<K, V> flatView() {
        return entryStream()
                .collect(StreamUtils.toLinkedMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delegate);
    }

    /**
     * Are these two delegating maps the same in terms of the current layer and their delegates are equal under the
     * definition of equality for that delegate.
     *
     * @param o  The object being compared
     *
     * @return true if the object being compared has the same local values and their delegates are equal
     */
    @Override
    public boolean equals(final Object o) {
        if (! (o instanceof DelegatingMap)) {
            return false;
        }
        DelegatingMap that = (DelegatingMap) o;
        return super.equals(that) && delegate.equals(that.delegate);
    }
}
