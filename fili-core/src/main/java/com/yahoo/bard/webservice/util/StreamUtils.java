// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Utils for dealing with streams.
 */
public class StreamUtils {
    /**
     * Returns a merge function, suitable for use in
     * {@link java.util.Map#merge(Object, Object, java.util.function.BiFunction) Map.merge()} or
     * {@link Collectors#toMap(Function, Function, BinaryOperator) toMap()}, which always
     * throws {@code IllegalStateException}.  This can be used to enforce the
     * assumption that the elements being collected are distinct.
     *
     * @param <T> the type of input arguments to the merge function
     *
     * @return a merge function which always throw {@code IllegalStateException}
     * @see Collectors#throwingMerger()
     */
    public static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); };
    }

    /**
     * Return a collector that creates a LinkedHashMap using the given key and value functions.
     * This collector assumes the elements being collected are distinct.
     *
     * @param <S>  Type of the objects being collected
     * @param <K>  Type of the keys
     * @param <V>  Type of the values
     * @param keyMapper  Mapping function for the key
     * @param valueMapper  Mapping function for the value
     *
     * @return a collector that creates a LinkedHashMap using the given key and value functions.
     * @throws IllegalStateException if multiple values are associated with the same key
     * @see Collectors#toMap(Function, Function, BinaryOperator, java.util.function.Supplier)
     */
    public static <S, K, V> Collector<S, ?, LinkedHashMap<K, V>> toLinkedMap(
            Function<? super S, ? extends K> keyMapper,
            Function<? super S, ? extends V> valueMapper
    ) {
        return Collectors.toMap(keyMapper, valueMapper, StreamUtils.throwingMerger(), LinkedHashMap::new);
    }

    /**
     * Return a collector that creates a LinkedHashMap using the given key and value functions.
     * This collector assumes the elements being collected are distinct.
     *
     * @param <S>  Type of the objects being collected
     * @param <K>  Type of the keys
     * @param <V>  Type of the values
     * @param <M>  The type of Map being collected into
     * @param keyMapper  Mapping function for the key
     * @param valueMapper  Mapping function for the value
     * @param mapSupplier  A function which returns a new, empty Map into which the results will be inserted
     *
     * @return a collector that creates a LinkedHashMap using the given key and value functions.
     * @throws IllegalStateException if multiple values are associated with the same key
     * @see Collectors#toMap(Function, Function, BinaryOperator, java.util.function.Supplier)
     */
    public static <S, K, V, M extends Map<K, V>> Collector<S, ?, M> toMap(
            Function<? super S, ? extends K> keyMapper,
            Function<? super S, ? extends V> valueMapper,
            Supplier<M> mapSupplier
    ) {
        return Collectors.toMap(keyMapper, valueMapper, StreamUtils.throwingMerger(), mapSupplier);
    }

    /**
     * Return a collector that creates a LinkedHashMap dictionary using the given key function.
     * This collector assumes the elements being collected are distinct.
     *
     * @param <S>  Type of the objects being collected and the values of the dictionary / map
     * @param <K>  Type of the keys
     * @param keyMapper  Mapping function for the key
     *
     * @return a collector that creates a LinkedHashMap dictionary using the given key function.
     * @throws IllegalStateException if multiple values are associated with the same key
     * @see Collectors#toMap(Function, Function, BinaryOperator, java.util.function.Supplier)
     */
    public static <S, K> Collector<S, ?, LinkedHashMap<K, S>> toLinkedDictionary(
            Function<? super S, ? extends K> keyMapper
    ) {
        return Collectors.toMap(keyMapper, Function.identity(), StreamUtils.throwingMerger(), LinkedHashMap::new);
    }

    /**
     * Return a collector that creates a dictionary using the given key function and the given map supplier.
     * This collector assumes the elements being collected are distinct.
     *
     * @param <S>  Type of the objects being collected and the values of the dictionary / map
     * @param <K>  Type of the keys
     * @param <M>  The type of Map being collected into
     * @param keyMapper  Mapping function for the key
     * @param mapSupplier  A function which returns a new, empty Map into which the results will be inserted
     *
     * @return a collector that creates a LinkedHashMap dictionary using the given key function.
     * @throws IllegalStateException if multiple values are associated with the same key
     * @see Collectors#toMap(Function, Function, BinaryOperator, java.util.function.Supplier)
     */
    public static <S, K, M extends Map<K, S>> Collector<S, ?, M> toDictionary(
            Function<? super S, ? extends K> keyMapper, Supplier<M> mapSupplier
    ) {
        return Collectors.toMap(keyMapper, Function.identity(), StreamUtils.throwingMerger(), mapSupplier);
    }

    /**
     * Negate the given predicate.
     *
     * @param <T>  Type of the predicate being negated
     * @param predicate  Predicate to negate
     *
     * @return the negated predicate
     */
    public static <T> Predicate<T> not(Predicate<T> predicate) {
        return predicate.negate();
    }

    /**
     * Return a function that can be used to do unchecked casts without generating a compiler warning.
     * <p>
     * Essentially the same as annotating the cast with {@code @SuppressWarnings("unchecked")}, which you can't do
     * inside a stream.
     *
     * @param targetType  Class to cast the operand (function parameter) to. Used only to set the generic type
     * @param <T> Operand type (type of function parameter)
     * @param <R> Cast type
     *
     * @return The function that will do the cast
     */
    public static <T, R> Function<T, R> uncheckedCast(Class<R> targetType) {
        return (T operand) -> {
                @SuppressWarnings("unchecked")
                R castedOperand = (R) operand;
                return castedOperand;
        };
    }

    /**
     * Build a function that can map a Map from one map to another.
     *
     * @param keyMapper  Function to map from the input Map's Entries to the result Map's keys
     * @param valueMapper  Function to map from the input Map's Entries to the result Map's values
     * @param resultSupplier  Supplier of the result map instance
     * @param <A>  Type of the input Map's keys
     * @param <B>  Type of the input Map's values
     * @param <K>  Type of the result Map's keys
     * @param <V>  Type of the result Map's values
     * @param <R>  Type of the input Map
     * @param <T>  Type of the result map
     *
     * @return a function that can map a Map from one map to another.
     */
    public static <A, B, K, V, R extends Map<A, B>, T extends Map<K, V>> Function<R, T> mapMap(
            Function<? super Map.Entry<A, B>, ? extends K> keyMapper,
            Function<? super Map.Entry<A, B>, ? extends V> valueMapper,
            Supplier<T> resultSupplier
    ) {
        return (R input) -> input.entrySet().stream()
                .collect(
                        resultSupplier,
                        (result, entry) -> result.put(keyMapper.apply(entry), valueMapper.apply(entry)),
                        Map::putAll
                );
    }

    /**
     * Build a function that can map a Map from one map to another.
     *
     * @param keyMapper  Function to map from the input Map's keys to the result Map's keys
     * @param resultSupplier  Supplier of the result map instance
     * @param <A>  Type of the input Map's keys
     * @param <B>  Type of the input Map's values
     * @param <K>  Type of the result Map's keys
     * @param <R>  Type of the input Map
     * @param <T>  Type of the result map
     *
     * @return a function that can map a Map from one map to another.
     */
    public static <A, B, K, R extends Map<A, B>, T extends Map<K, B>> Function<R, T> mapMapKey(
            Function<? super A, ? extends K> keyMapper,
            Supplier<T> resultSupplier
    ) {
        return mapMap(entry -> keyMapper.apply(entry.getKey()), Map.Entry::getValue, resultSupplier);
    }

    /**
     * Build a function that can map a Map from one map to another.
     *
     * @param valueMapper  Function to map from the input Map's values to the result Map's values
     * @param resultSupplier  Supplier of the result map instance
     * @param <A>  Type of the input Map's keys
     * @param <B>  Type of the input Map's values
     * @param <V>  Type of the result Map's values
     * @param <R>  Type of the input Map
     * @param <T>  Type of the result map
     *
     * @return a function that can map a Map from one map to another.
     */
    public static <A, B, V, R extends Map<A, B>, T extends Map<A, V>> Function<R, T> mapMapValue(
            Function<? super B, ? extends V> valueMapper,
            Supplier<T> resultSupplier
    ) {
        return mapMap(Map.Entry::getKey, entry -> valueMapper.apply(entry.getValue()), resultSupplier);
    }

    /**
     * Build a function that can map a Map from one map to another.
     * <p>
     * Makes no guarantees on the type of Map produced.
     *
     * @param keyMapper  Function to map from the input Map's Entries to the result Map's keys
     * @param valueMapper  Function to map from the input Map's Entries to the result Map's values
     * @param <A>  Type of the input Map's keys
     * @param <B>  Type of the input Map's values
     * @param <K>  Type of the result Map's keys
     * @param <V>  Type of the result Map's values
     * @param <R>  Type of the input Map
     *
     * @return a function that can map a Map from one map to another.
     */
    public static <A, B, K, V, R extends Map<A, B>> Function<R, Map<K, V>> mapMap(
            Function<? super Map.Entry<A, B>, ? extends K> keyMapper,
            Function<? super Map.Entry<A, B>, ? extends V> valueMapper
    ) {
        return (R input) -> input.entrySet().stream()
                .collect(
                        HashMap::new,
                        (result, entry) -> result.put(keyMapper.apply(entry), valueMapper.apply(entry)),
                        Map::putAll
                );
    }

    /**
     * Build a function that can map a Map from one map to another.
     * <p>
     * Makes no guarantees on the type of Map produced.
     *
     * @param keyMapper  Function to map from the input Map's keys to the result Map's keys
     * @param <A>  Type of the input Map's keys
     * @param <B>  Type of the input Map's values
     * @param <K>  Type of the result Map's keys
     * @param <R>  Type of the input Map
     *
     * @return a function that can map a Map from one map to another.
     */
    public static <A, B, K, R extends Map<A, B>> Function<R, Map<K, B>> mapMapKey(
            Function<? super A, ? extends K> keyMapper
    ) {
        return mapMap(entry -> keyMapper.apply(entry.getKey()), Map.Entry::getValue, HashMap::new);
    }

    /**
     * Build a function that can map a Map from one map to another.
     * <p>
     * Makes no guarantees on the type of Map produced.
     *
     * @param valueMapper  Function to map from the input Map's values to the result Map's values
     * @param <A>  Type of the input Map's keys
     * @param <B>  Type of the input Map's values
     * @param <V>  Type of the result Map's values
     * @param <R>  Type of the input Map
     *
     * @return a function that can map a Map from one map to another.
     */
    public static <A, B, V, R extends Map<A, B>> Function<R, Map<A, V>> mapMapValue(
            Function<? super B, ? extends V> valueMapper
    ) {
        return mapMap(Map.Entry::getKey, entry -> valueMapper.apply(entry.getValue()), HashMap::new);
    }
}
