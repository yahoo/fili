// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.mutable;

import com.yahoo.bard.webservice.data.dimension.rfc_dimension.KeyValueStore;

import org.slf4j.LoggerFactory;


// A replaceable KVS is not necessarily Mutable.  Mutable means that it can accept modifications while active,
// replaceability relies on an external file.
public interface ReplaceableKeyValueStore<K, V> extends KeyValueStore<K, V> {
    /**
     * Replaces key value store with a new key value store.
     *
     * @see {@link com.yahoo.bard.webservice.data.dimension.KeyValueStore#replaceStore(String)}
     *
     * @param newStorePath  The location of the new store.
     */
    default void replaceStore(String newStorePath) {
        String message = String.format(
                "Current implementation of KeyValueStore: %s does not support replacement operation.",
                this.getClass().getSimpleName()
        );
        LoggerFactory.getLogger(KeyValueStore.class).error(message);
        throw new UnsupportedOperationException(message);
    }
}
