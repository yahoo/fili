// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.data.config.metric.MetricInstance;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Utils.
 */
public class Utils {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    /**
     * A strategy that deletes a directory.
     */
    private static final FileVisitor<Path> DELETE_VISITOR = new SimpleFileVisitor<Path>() {
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
    };

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
                .filter(type::isInstance)
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
     * @param e  The array from which the LinkedHashSet will be built
     *
     * @return a LinkedHashSet view of the specified array
     */
    @SafeVarargs
    public static <E> LinkedHashSet<E> asLinkedHashSet(E... e) {
        return new LinkedHashSet<>(Arrays.asList(e));
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
     * Delete files or directories in the specified path.
     *
     * @param path  The pathname
     */
    public static void deleteFiles(String path) {
        Path file = Paths.get(path);

        // do nothing if there is nothing to delete
        if (!Files.exists(file)) {
            LOG.trace(String.format("'%s' does not exist. Nothing is deleted", path));
            return;
        }

        try {
            Files.walkFileTree(file, DELETE_VISITOR);
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
     * @param fieldName The name of the node to be omitted.
     * @param mapper  The object mapper that creates and empty node.
     *
     * @deprecated  Should avoid this method and instead use {@link #canonicalize(JsonNode, ObjectMapper, boolean)}
     * which preserves JSON object ordering that guarantees consistent hash values.
     */
    @Deprecated
    public static void omitField(JsonNode node, String fieldName, ObjectMapper mapper) {
        if (node.has("context")) {
            ((ObjectNode) node).replace(fieldName, mapper.createObjectNode());
        }

        for (JsonNode child : node) {
            omitField(child, fieldName, mapper);
        }
    }

    /**
     * Given a JsonObjectNode, order the fields and recursively and replace context blocks with empty nodes.
     *
     * This method is recursive.
     *
     * @param node  The root of the tree of json nodes.
     * @param mapper  The object mapper that creates and empty node.
     * @param preserveContext  Boolean indicating whether context should be omitted.
     */
    public static void canonicalize(JsonNode node, ObjectMapper mapper, boolean preserveContext) {
        if (node.isObject()) {
            ObjectNode objectNode = ((ObjectNode) node);

            if (objectNode.has("context") && !preserveContext) {
                objectNode.replace("context", mapper.createObjectNode());
            }

            Iterator<Map.Entry<String, JsonNode>> iterator = objectNode.fields();
            // collect and sort the entries
            TreeMap<String, JsonNode> fieldMap = new TreeMap<>();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                fieldMap.put(entry.getKey(), entry.getValue());
                // canonicalize all child nodes
                canonicalize(entry.getValue(), mapper, preserveContext);
            }
            // remove the existing entries
            objectNode.removeAll();

            // replace the entries in sorted order
            objectNode.setAll(fieldMap);
        }
    }

    /**
     * Find the minimum value between two comparable objects.
     *
     * @param one  Item 1
     * @param two  Item 2
     * @param <T>  Type of object to compare
     *
     * @return the minimum of the two objects
     */
    @SuppressWarnings("unchecked")
    public static <T extends Comparable> T getMinValue(T one, T two) {
        return one.compareTo(two) < 1 ? one : two;
    }

    /**
     * Given an ImmutablePair, and a right value, returns a new ImmutablePair with the same left value,
     * and the specified right value.
     *
     * @param pair  Immutable Pair instance
     * @param right  The right value, may be null
     * @param <T>  Left type of the pair
     * @param <U>  Right type of the pair
     * @param <V>  The right value to have new Immutable Pair
     *
     * @return New instance of Immutable Pair
     */
    public static <T, U, V> ImmutablePair<T, V> withRight(ImmutablePair<T, U> pair, V right) {
        return new ImmutablePair<>(pair.getLeft(), right);
    }

    /**
     * Create metrics from instance descriptors and store in the metric dictionary.
     *
     * @param metricDictionary  The dictionary to store metrics in
     * @param metrics  The list of metric descriptors
     */
    public static void addToMetricDictionary(MetricDictionary metricDictionary, List<MetricInstance> metrics) {
        metrics.stream().map(MetricInstance::make).forEach(metricDictionary::add);
    }
}
