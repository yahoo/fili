package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.ApiFilter;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class UnmodifiableApiFilters extends ApiFilters {

    private final Map<Dimension, Set<ApiFilter>> target;

    private UnmodifiableApiFilters(LinkedHashMap<Dimension, Set<ApiFilter>> filters) {
        this.target = Collections.unmodifiableMap(new ApiFilters(filters));
    }

    public static UnmodifiableApiFilters of(ApiFilters target) {
        return new UnmodifiableApiFilters(target);
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
    @Override public Set<ApiFilter> get(Object key) {
        return target.get(key);
    }

    @Override public Set<ApiFilter> put(Dimension key, Set<ApiFilter> value) {return target.put(key, value); }
    @Override public Set<ApiFilter> remove(Object key) {
        return target.remove(key);
    }
    @Override public void putAll(Map<? extends Dimension, ? extends Set<ApiFilter>> m) {
        target.putAll(m);
    }
    @Override public void clear() {
        target.clear();
    }
    @Override public Set<Dimension> keySet() {
        return target.keySet();
    }
    @Override public Set<Map.Entry<Dimension,Set<ApiFilter>>> entrySet() {
        return target.entrySet();
    }
    @Override public Collection<Set<ApiFilter>> values() {
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

    @Override @SuppressWarnings("unchecked") public Set<ApiFilter> getOrDefault(Object k, Set<ApiFilter> defaultValue) {
        return target.getOrDefault(k, defaultValue);
    }

    @Override public void forEach(BiConsumer<? super Dimension, ? super Set<ApiFilter>> action) {
        target.forEach(action);
    }

    @Override public void replaceAll(
            BiFunction<? super Dimension, ? super Set<ApiFilter>, ? extends Set<ApiFilter>> function
    ) {
        target.replaceAll(function);
    }

    @Override public Set<ApiFilter> putIfAbsent(Dimension key, Set<ApiFilter> value) {
        return target.putIfAbsent(key, value);
    }

    @Override public boolean remove(Object key, Object value) {
        return target.remove(key, value);
    }

    @Override public boolean replace(Dimension key, Set<ApiFilter> oldValue, Set<ApiFilter> newValue) {
        return target.replace(key, oldValue, newValue);
    }

    @Override public Set<ApiFilter> replace(Dimension key, Set<ApiFilter> value) {
        return target.replace(key, value);
    }

    @Override public Set<ApiFilter> computeIfAbsent(
            Dimension key,
            Function<? super Dimension, ? extends Set<ApiFilter>> mappingFunction
    ) {
        return target.computeIfAbsent(key, mappingFunction);
    }

    @Override public Set<ApiFilter> computeIfPresent(
            Dimension key,
            BiFunction<? super Dimension, ? super Set<ApiFilter>, ? extends Set<ApiFilter>> remappingFunction
    ) {
        return target.computeIfPresent(key, remappingFunction);
    }

    @Override public Set<ApiFilter> compute(
            Dimension key,
            BiFunction<? super Dimension, ? super Set<ApiFilter>, ? extends Set<ApiFilter>> remappingFunction
    ) {
        return target.compute(key, remappingFunction);
    }

    @Override public Set<ApiFilter> merge(
            Dimension key,
            Set<ApiFilter> value,
            BiFunction<? super Set<ApiFilter>, ? super Set<ApiFilter>, ? extends Set<ApiFilter>> remappingFunction
    ) {
        return target.merge(key, value, remappingFunction);
    }
}
