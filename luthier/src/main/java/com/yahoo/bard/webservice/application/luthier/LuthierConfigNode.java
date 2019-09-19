// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.application.luthier;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;

/**
 * An interface into the unstructured blobs of configuration that the Luthier logic works with. It mimics a
 * subset of Jackson's ObjectNode's interface, but provides an easy-to-implement layer of indirection behind
 * which other schemes (such as one using Luaj) can leverage.
 * <p>
 * This interface extends Iterable. It should iterate over the values of the immediate children of this node.
 */
public interface LuthierConfigNode extends Iterable<LuthierConfigNode> {

    /**
     * Returns the configuration node associated with the given key.
     *
     * @param key The name of the desired node
     *
     * @return A LuthierConfigNode associated with that key, or null if there is none.
     */
    LuthierConfigNode get(String key);

    /**
     * Returns a spliterator over this node's immediate children.
     *
     * If this node represents a leaf (i.e. a String, or a number), then the
     * returned spliterator is empty.
     *
     * @return A spliterator over the immediate children of this node
     */
    default Spliterator<LuthierConfigNode> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(), 0);
    }

    /**
     * Returns an iterator over the field names of this configuration node. If
     * the underlying node is not a map, then the iterator is empty
     *
     * @return An iterator over all the field names in this node
     */
    Iterator<String> fieldNames();

    /**
     * Checks whether this node has the given field.
     *
     * @param fieldName The field to check for
     *
     * @return True if the node has that field, false otherwise
     */
    boolean has(String fieldName);

    /**
     * If this node wraps a String, returns the wrapped String.
     *
     * @return The string wrapped by this node, or null if the node is not a string
     */
    String textValue();

    /**
     * If this node wraps a boolean, returns the wrapped boolean.
     *
     * @return The boolean wrapped by this node, or false if the node is not a boolean
     */
    boolean booleanValue();

    /**
     * If this node wraps a long, returns the wrapped long.
     *
     * @return The long wrapped by this node, or 0 if the node is not a number
     */
    long longValue();

    /**
     * If this node wraps a int, returns the wrapped int.
     *
     * @return The long wrapped by this node, or 0 if the node is not a number
     */
    int intValue();
}
