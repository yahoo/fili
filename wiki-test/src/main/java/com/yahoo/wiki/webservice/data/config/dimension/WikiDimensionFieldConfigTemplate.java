// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.wiki.webservice.data.config.Template;

import java.util.LinkedHashSet;

/**
 * Contains field name and a list of field template.
 * For DimensionFieldDeserializer
 */
public class WikiDimensionFieldConfigTemplate extends Template {

    private String fieldName;
    private LinkedHashSet<WikiDimensionFieldSetsTemplate> fieldList;

    /**
     * Constructor.
     */
    WikiDimensionFieldConfigTemplate() {
    }

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
    public void setFieldList(LinkedHashSet<WikiDimensionFieldSetsTemplate> list) {
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
    public LinkedHashSet<WikiDimensionFieldSetsTemplate> getFieldList() {
        return fieldList;
    }
}
