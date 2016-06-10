// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * DimensionRow is the model for a row in a Dimension lookup table.
 */
public class DimensionRow extends LinkedHashMap<DimensionField, String> implements Comparable<DimensionRow> {
    private String keyvalue;

    /**
     * Build a dimension row with a key field value and a map of field values.
     *
     * @param key  the field which will be used as a the data source key for this dimension
     * @param fieldValueMap  A map of all values on this row, keyed by field
     */
    public DimensionRow(@NotNull DimensionField key, Map<DimensionField, String> fieldValueMap) {
        super(fieldValueMap);
        this.keyvalue = fieldValueMap.get(key);
        if (keyvalue == null) {
            throw new IllegalArgumentException("Missing key " + key);
        }
    }

    @Override
    public int compareTo(DimensionRow that) {
        if (this == that) { return 0; }
        int c = this.keyvalue.compareTo(that.keyvalue);
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
        LinkedHashMap<String, String> row = new LinkedHashMap<>(this.size());
        for (Map.Entry<DimensionField, String> fieldValueEntry : this.entrySet()) {
            String name = fieldValueEntry.getKey().getName();
            row.put(name, fieldValueEntry.getValue());
        }
        return row;
    }
}
