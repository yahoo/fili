// Copyright 2021 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.validation.constraints.NotNull;

public class MetricType {

    private final String type;

    private final String subType;

    Map<String, String> typeMetadata;

    /**
     * Constructor.
     *
     * @param type The name of the type
     * @param subType A subtype
     * @param typeMetadata metadata attached to this instance of type.
     */
    public MetricType(@NotNull String type, String subType, Map<String, String> typeMetadata) {
        this.type = type;
        this.subType = subType;
        Map<String, String> metadata = new HashMap<>();
        if (typeMetadata != null) {
            metadata.putAll(typeMetadata);
        }
        this.typeMetadata = Collections.unmodifiableMap(metadata);
    }

    public MetricType(final String type) {
        this(type, null, null);
    }

    public MetricType(final String type, final String subType) {
        this(type, subType, null);
    }

    public String getType() {
        return type;
    }

    public String getSubType() {
        return subType;
    }

    public Map<String, String> getTypeMetadata() {
        return typeMetadata;
    }

    public MetricType withSubtype(String subType) {
        return new MetricType(type, subType, typeMetadata);
    }

    public MetricType withMetadata(Map<String, String> typeMetadata) {
        return new MetricType(type, subType, typeMetadata);
    }

    public MetricType withKeyValue(String key, String value) {
        Map<String, String> metadata = new HashMap<>(typeMetadata);
        if (value == null) {
            metadata.remove(key);
        } else {
            metadata.put(key, value);
        }
        return withMetadata(metadata);
    }

    public String toString() {
        return String.format(
                "Type: %s, Subtype: %s, Params: %s",
                type,
                subType == null ? "" : subType,
                typeMetadata.toString()
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) { return true; }
        if (!(o instanceof MetricType)) { return false; }
        final MetricType that = (MetricType) o;
        return type.equals(that.type) &&
                Objects.equals(subType, that.subType) &&
                typeMetadata.equals(that.typeMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, subType, typeMetadata);
    }
}
