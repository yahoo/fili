package com.yahoo.wiki.webservice.data.config.dimension;

import com.yahoo.wiki.webservice.data.config.Template;

import java.util.LinkedHashSet;

/**
 * Contains field name and a list of field template
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
     * Set field info
     */
    public void setFieldName(String name) {
        this.fieldName = name;
    }

    public void setFieldList(LinkedHashSet<WikiDimensionFieldSetsTemplate> list) {
        this.fieldList = list;
    }

    /**
     * Get field info
     */
    public String getFieldName() {
        return this.fieldName;
    }

    public LinkedHashSet<WikiDimensionFieldSetsTemplate> getFieldList() {
        return fieldList;
    }
}
