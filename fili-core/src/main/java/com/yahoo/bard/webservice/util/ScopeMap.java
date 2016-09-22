// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ScopeMap implements {@link Scope Scope} using a factory method to create child scopes when requested by getScope()
 * <p>
 * In general query usage, the ScopeMap representing the global scope will be passed and clients will look up the child
 * scope which they wish to use to resolve map keys or store map keys at.
 *
 * @param <S> The address element type for looking up child scopes
 * @param <K> The key type for the map
 * @param <V> The value type for the map
 * @param <T> The implementation type (used to type returns from getScope in subclasses)
 */
abstract public class ScopeMap<S, K, V, T extends ScopeMap<S, K, V, T>> extends DelegatingMap<K, V>
        implements Scope<S, K, V, T> {

    /**
     * Child scopes of this scope.
     */
    private final Map<S, T> scopes = new LinkedHashMap<>();

    /**
     * Build a scope with no parent scope and empty children.
     */
    public ScopeMap() {
        super();
    }

    /**
     * Build a scope with a parent scope.
     *
     * @param parent  The map to delegate key lookups to
     */
    public ScopeMap(Map<K, V> parent) {
        super(parent);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T getScope(List<S> scopeKeys) {
        return scopeKeys.isEmpty() ?
                (T) this :
                scopes.computeIfAbsent(scopeKeys.get(0), ((ignored) -> factory((T) this)))
                        .getScope(scopeKeys.subList(1, scopeKeys.size()));
    }

    /**
     * Helper function converting the varargs to array.
     *
     * @param scopeKeys  The subtree address expressed as a sequence of scopeKeys
     *
     * @return a scope, which is a subtree of this scope
     */
    public T getScope(S... scopeKeys) {
        return getScope(Arrays.asList(scopeKeys));
    }


    /**
     * Used to construct child scopes.
     *
     * @param scope The parent scope for the scope being created
     *
     * @return A new Scope which is a child of this scope
     */
    protected abstract T factory(T scope);
}
