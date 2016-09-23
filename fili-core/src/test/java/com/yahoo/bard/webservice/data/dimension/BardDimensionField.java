// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * A dimension field used in bard test configuration.
 */
public enum BardDimensionField implements DimensionField {
    ID("Dimension ID"),
    DESC("Dimension Description"),
    FIELD1("Dimension field 1"),
    FIELD2("Dimension field 2");

    private String description;
    private String camelName;

    /**
     * Constructor.
     *
     * @param description  Human-consumable description of the field
     */
    BardDimensionField(String description) {
        this.description = description;
        camelName = EnumUtils.camelCase(name());
    }

    @Override
    public String getName() {
        return camelName;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return camelName;
    }

    /**
     * Make test DimensionRow using ID and DESC fields from provided Dimension.
     *
     * @param dimension  provided dimension
     * @param values  Values for dimension fields
     *
     * @return test DimensionRow
     */
    public static DimensionRow makeDimensionRow(Dimension dimension, String...values) {
        LinkedHashMap<DimensionField, String> map = new LinkedHashMap<>(values.length);
        Iterator<DimensionField> fields = dimension.getDimensionFields().iterator();
        for (String value : values) {
            DimensionField field = fields.next();
            map.put(field, value);
        }
        return new DimensionRow(dimension.getKey(), map);
    }
}
