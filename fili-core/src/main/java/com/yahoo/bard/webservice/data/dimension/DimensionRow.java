// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.util.StreamUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import javax.validation.constraints.NotNull;

/**
 * DimensionRow is the model for a row in a Dimension lookup table.
 */
public class DimensionRow extends LinkedHashMap<DimensionField, String> implements Comparable<DimensionRow> {

    private final DimensionField key;
    private final String keyValue;

    /**
     * Build a copy of a dimension row.
     *
     * @param row  the dimension row to be copied
     */
    private DimensionRow(@NotNull DimensionRow row) {
        super(row);
        this.key = row.key;
        this.keyValue = row.keyValue;
    }

    /**
     * Build a dimension row with a key field value and a map of field values.
     *
     * @param key  the field which will be used as a the data source key for this dimension
     * @param fieldValueMap  A map of all values on this row, keyed by field
     */
    public DimensionRow(@NotNull DimensionField key, Map<DimensionField, String> fieldValueMap) {
        super(fieldValueMap);
        this.key = key;
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
    public LinkedHashMap<String, String> getRowMap() {
        return entrySet().stream()
                .collect(StreamUtils.toLinkedMap(entry -> entry.getKey().getName(), Map.Entry::getValue));
    }

    /**
     * Copies a DimensionRow and transforms its fields using the specified mapper.
     *
     * @param row The row to be transformed
     * @param mapper  A function that takes a DimensionField and its current value, and returns the field's new value.
     *
     * @return A copy of the DimensionRow with its fields transformed by the specified function
     */
    public static DimensionRow copyWithReplace(DimensionRow row,  BiFunction<DimensionField, String, String> mapper) {
        DimensionRow newRow = new DimensionRow(row);
        newRow.replaceAll(mapper);
        return newRow;
    }
}
