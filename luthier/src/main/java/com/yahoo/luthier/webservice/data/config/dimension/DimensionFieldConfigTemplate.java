// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import java.util.LinkedHashSet;

/**
 * Contains field name and a list of field template.
 * For DimensionFieldDeserializer
 * <p>
 *        field name: "xxx"
 *        field list: [field A, field B, field C]
 */
public class DimensionFieldConfigTemplate {

    private String fieldName;
    private LinkedHashSet<DimensionFieldSetsTemplate> fieldList;

    /**
     * Set fieldset name.
     *
     * @param name fieldset name
     */
    public void setFieldName(String name) {
        this.fieldName = name;
    }

    /**
     * Set field list.
     *
     * @param list a list of field
     */
    public void setFieldList(LinkedHashSet<DimensionFieldSetsTemplate> list) {
        this.fieldList = list;
    }

    /**
     * Get fieldset name.
     *
     * @return fieldset name
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * Get field list.
     *
     * @return a list of field
     */
    public LinkedHashSet<DimensionFieldSetsTemplate> getFieldList() {
        return fieldList;
    }
}
