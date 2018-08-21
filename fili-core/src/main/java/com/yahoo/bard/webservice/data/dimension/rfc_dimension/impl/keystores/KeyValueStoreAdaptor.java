// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension.impl.keystores;

import com.yahoo.bard.webservice.data.dimension.rfc_dimension.mutable.MutableKeyValueStore;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class KeyValueStoreAdaptor implements ByteArrayKeyValueStore,  MutableKeyValueStore<String, byte[]> {

    com.yahoo.bard.webservice.data.dimension.KeyValueStore keyValueStore;

    public KeyValueStoreAdaptor(com.yahoo.bard.webservice.data.dimension.KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    @Override
    public byte[] get(String key) {
        return keyValueStore.get(key).getBytes();
    }

    @Override
    public String put(String key, byte[] value) {
        return keyValueStore.put(key, new String(value));
    }

    @Override
    public Map<String, byte[]> putAll(Map<String, byte[]> entries) {
        Map<String, String> values = entries.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, it -> new String(it.getValue())));
        return keyValueStore.putAll(values).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().getBytes()));
    }

    @Override
    public byte[] remove(String key) {
        return keyValueStore.remove(key).getBytes();
    }

    @Override
    public void open() {
        keyValueStore.open();
    }

    @Override
    public void close() {
        keyValueStore.close();
    }

    @Override
    public boolean isOpen() {
        return keyValueStore.isOpen();
    }

    @Override
    public boolean isHealthy() {
        return keyValueStore.isHealthy();
    }

    @Override
    public Optional<Integer> getCardinality() {
        return Optional.empty();
    }
}
