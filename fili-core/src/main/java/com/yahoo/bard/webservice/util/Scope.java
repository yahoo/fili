// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A Scope represents a tree of delegating maps.
 * <p>
 * A Scope may have child scopes which it encloses. It may have a parent scope which encloses it. Values defined in
 * a parent scope will be available via (@link Map#get(K k)} on itself, unless the scope itself defines (and therefore
 * overshadows) that entry.
 * <p>
 * Changes to a scope will be available to it's child scopes unless they overshadow the corresponding key.
 * <p>
 * The remove and clear operations are not supported on Scopes because parent scopes cannot be directly
 * accessed or altered.
 *
 * @param <S> The address element type for looking up child scopes
 * @param <K> The key type for the map
 * @param <V> The value type for the map
 * @param <T> The implementation type (used to type returns from getScope in subclasses)
 */
public interface Scope<S, K, V, T extends Scope<S, K, V, T>> extends Map<K, V> {

    /**
     * Resolves a child scope.
     * The child scope should delegate to its parent scopes for value lookups.
     * <p>
     * Unless otherwise noted implementations should guarantee lazy initialization of child scopes.
     *
     * @param scopeKeys  The subtree address expressed as a sequence of scopeKeys
     *
     * @return A scope which is a subtree of this scope.
     */
    T getScope(List<S> scopeKeys);

    /**
     * Puts a key value pair into an immediate child scope.
     *
     * @param scopeKey  The name of the child scope
     * @param key The key for the value being stored
     * @param value The value being stored
     *
     * @return The value displaced or shadowed {@link DelegatingMap#put(Object, Object)}
     */
    default V putScope(S scopeKey, K key, V value) {
        return getScope(Collections.singletonList(scopeKey)).put(key, value);
    }
}
