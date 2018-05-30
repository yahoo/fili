// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.keystores;

import com.yahoo.bard.webservice.data.dimension.rfc_dimension.KeyValueStore;

import java.util.Optional;


/**
 * This should probably replace String value KeyValueStore as the baseline KVS
 */
public interface ByteArrayKeyValueStore extends KeyValueStore<String, byte[]> {
    Optional<Integer> getCardinality();
}
