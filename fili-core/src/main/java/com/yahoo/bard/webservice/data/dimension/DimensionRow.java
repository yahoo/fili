// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.util.StreamUtils;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * DimensionRow is the model for a row in a Dimension lookup table.
 */
@JsonSerialize(using = DimensionRowSerializer.class)
public class DimensionRow extends LinkedHashMap<DimensionField, String> implements Comparable<DimensionRow> {

    private final String keyValue;

    /**
     * Build a dimension row with a key field value and a map of field values.
     *
     * @param key  the field which will be used as a the data source key for this dimension
     * @param fieldValueMap  A map of all values on this row, keyed by field
     */
    public DimensionRow(@NotNull DimensionField key, Map<DimensionField, String> fieldValueMap) {
        super(fieldValueMap);
        this.keyValue = fieldValueMap.get(key);
        if (keyValue == null) {
            throw new IllegalArgumentException("Missing key " + key);
        }
    }

    /**
     * Getter.
     *
     * @return The value of the key field for this dimension row
     */
    public String getKeyValue() {
        return keyValue;
    }

    @Override
    public int compareTo(DimensionRow that) {
        if (this == that) { return 0; }
        int c = this.keyValue.compareTo(that.keyValue);
        if (c == 0) {
            for (DimensionField k : this.keySet()) {
                c = String.valueOf(this.get(k)).compareTo(String.valueOf(that.get(k)));
                if (c != 0) {
                    break;
                }
            }
        }
        return c;
    }

    /**
     * Get DimensionRows as a map of Field Name and Value.
     *
     * @return map of fieldname,value
     */
    public Map<String, String> getRowMap() {
        return entrySet().stream()
                .collect(StreamUtils.toLinkedMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
    }
}
