// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.dimension.extractionfunction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

/**
 * Lookup property of Lookup Extraction Function using provided map as key value mapping.
 */
public class MapLookup extends Lookup {
    private final Map<String, String> mapping;

    /**
     * Constructor.
     *
     * @param mapping  dimension value key to target dimension value mapping provided by the user.
     */
    public MapLookup(Map<String, String> mapping) {
        super("map");
        this.mapping = ImmutableMap.copyOf(mapping);
    }

    @JsonProperty(value = "map")
    public Map<String, String> getMapping() {
        return mapping;
    }

    // CHECKSTYLE:OFF
    public MapLookup withMapping(Map<String, String> mapping) {
        return new MapLookup(mapping);
    }
    // CHECKSTYLE:ON

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), mapping);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null || getClass() != obj.getClass()) { return false; }
        MapLookup other = (MapLookup) obj;
        return super.equals(obj) && Objects.equals(mapping, other.mapping);
    }
}
