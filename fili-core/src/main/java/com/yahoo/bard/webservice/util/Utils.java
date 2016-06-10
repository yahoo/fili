// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Utils.
 */
public class Utils {
    /**
     * Given a collection of objects which share the same super class, return the subset of objects that share a common
     * sub class.
     *
     * @param set  Input set
     * @param <T> Input set type
     * @param type  sub class
     * @param <K> sub class type
     *
     * @return ordered subset of objects that share a common sub class
     */
    public static <T, K extends T> LinkedHashSet<K> getSubsetByType(Collection<T> set, Class<K> type) {
        return set.stream()
                .filter(member -> type.isAssignableFrom(member.getClass()))
                .map(StreamUtils.uncheckedCast(type))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Easily turn a few instances of an object into a LinkedHashSet.
     *
     * <pre>
     * LinkedHashSet&lt;String&gt; stooges = Utils.asLinkedHashSet("Larry", "Moe", "Curly");
     * </pre>
     *
     * @param <E>  The element type for the linked hash set
     * @param e  the array from which the LinkedHashSet will be built
     *
     * @return a LinkedHashSet view of the specified array
     */
    @SafeVarargs
    public static <E> LinkedHashSet<E> asLinkedHashSet(E... e) {
        return new LinkedHashSet<>(Arrays.asList(e));
    }

    /**
     * Helper class for wrapper around a Collection to treat as an unmodifiable Set.
     *
     * @param <E> the type of elements maintained by this set
     */
    static class CollectionAsUnmodifiableSet<E> extends AbstractSet<E> {

        private static final Logger LOG = LoggerFactory.getLogger(CollectionAsUnmodifiableSet.class);

        Collection<E> collection;

        CollectionAsUnmodifiableSet(@NotNull Collection<E> c) {
            // Check for null collection
            if (c == null) {
                String message = "Collection cannot be null";
                LOG.error(message);
                throw new IllegalArgumentException(message);
            }
            this.collection = c;
        }

        @Override
        public Iterator<E> iterator() {
            return collection.iterator();
        }

        @Override
        public int size() {
            return collection.size();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(E e) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Create parent directories if they don't exist in a given file path.
     *
     * @param path  The pathname
     */
    public static void createParentDirectories(String path) {
        File targetFile = new File(path);
        File parent = targetFile.getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("Couldn't create dir: " + parent);
        }
    }

    /**
     * An terminator to the recursive making of immutable collection trees.
     *
     * @param <T>  The type of the collection
     * @param nonCollection  An object that isn't a map or collection
     *
     * @return The object itself
     */
    private static <T> T makeImmutable(T nonCollection) {
        return nonCollection;
    }

    /**
     * A recursive call to make a map and all it's values immutable.
     *
     * @param <K>  The key type of the map
     * @param <V>  The value type of the map
     * @param mutableMap  The original, potentially mutable, map
     *
     * @return An immutable Map with all it's values made immutable.
     */
    public static <K, V> Map<K, V> makeImmutable(Map<K, V> mutableMap) {
        Map<K, V> newMap = new HashMap<>();
        for (Map.Entry<K, V>  entry : mutableMap.entrySet()) {
            newMap.put(entry.getKey(), Utils.makeImmutable(entry.getValue()));
        }
        return Collections.unmodifiableMap(newMap);
    }

    /**
     * A recursive call to make a collection and all it's values immutable.
     *
     * @param <T>  The type of the collection
     * @param mutableCollection  The original, potentially mutable, collection
     *
     * @return Am immutable copy whose values are also immutable.
     */
    public static <T> Collection<T> makeImmutable(Collection<T> mutableCollection) {
        Collection<T> newCollection;
        try {
            newCollection = mutableCollection.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        for (T element : mutableCollection) {
            newCollection.add(Utils.makeImmutable(element));
        }
        return Collections.unmodifiableCollection(newCollection);
    }

    /**
     * Delete files or directories in the specified path.
     *
     * @param path  The pathname
     */
    public static void deleteFiles(String path) {
        Path directory = Paths.get(path);

        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Helper method to throw an exception when a result is expected as a return value (e.g. in a ternary operator)
     *
     * @param <T>  The type of exception being returned
     * @param exception  The exception to be thrown
     *
     * @return is only used for type inference. No object is actually ever returned. An exception is always being thrown
     * instead
     */
    public static <T> T insteadThrowRuntime(RuntimeException exception) {
        throw exception;
    }

    /**
     * Helper method to return request headers as a map of the same type with its keys lower cased.
     *
     * @param headers  The request headers.
     *
     * @return The headers with their names lower cased.
     */
    public static MultivaluedMap<String, String> headersToLowerCase(MultivaluedMap<String, String> headers) {
        return headers.entrySet().stream()
                .collect(
                        StreamUtils.toMap(
                                e -> e.getKey().toLowerCase(Locale.ENGLISH),
                                Map.Entry::getValue,
                                MultivaluedHashMap::new
                        )
                );
    }

    /**
     * Given a field name and a tree of json nodes, empty the contents of all the json nodes matching the field name.
     * This method is recursive.
     *
     * @param node The root of the tree of json nodes.
     * @param fieldName The name of the node to be emitted.
     * @param mapper  The object mapper that creates and empty node.
     */
    public static void emitField(JsonNode node, String fieldName, ObjectMapper mapper) {
        if (node.has("context")) {
            ((ObjectNode) node).replace(fieldName, mapper.createObjectNode());
        }

        for (JsonNode child : node) {
            emitField(child, fieldName, mapper);
        }
    }

    public static <T extends Comparable> T getMinValue(T one, T two) {
        return one.compareTo(two) < 1 ? one : two;
    }
}
