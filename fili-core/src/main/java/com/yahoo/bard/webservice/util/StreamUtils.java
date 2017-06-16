// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * Collect a stream into an unmodifiable set preserving first-in ordering.
     *
     * @param <T> The type of the value stream
     * @param values  The stream of values to be collected
     *
     * @return An unmodifiable copy of a linked hash set view of the stream
     */
    public static <T> Set<T> toUnmodifiableSet(Stream<T> values) {
        return Collections.unmodifiableSet((Set<T>) values.collect(Collectors.toCollection(LinkedHashSet::new)));
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
     * Copy a set, add a value to it and return the new set.
     *
     * @param set  The original set being copied.
     * @param value  The value being appended to the set.
     * @param <T>  The type of the set
     *
     * @return the new set containing the additional value
     */
    public static <T> Set<T> append(Set<T> set, T value) {
        HashSet<T> result = new HashSet<T>(set);
        result.add(value);
        return result;
    }

    /**
     * Merge two sets without modifying either.
     * This method implements {@link BinaryOperator} for use in stream reductions.
     *
     * @param a  One input set
     * @param b  Another input set
     * @param <T>  The type of the underlying sets
     *
     * @return  A new set containing the values of the original sets
     */
    public static <T> Set<T> setMerge(Set<T> a, Set<T> b) {
        return Stream.concat(a.stream(), b.stream()).collect(Collectors.toSet());
    }
}
