package com.yahoo.wiki.webservice.data.config.dimension;

import java.util.LinkedHashSet;

/**
 * Contains field name and a list of field template
 * For DimensionFieldDeserializer
 */
public class WikiDimensionField{

    private String fieldName;
    private LinkedHashSet<WikiDimensionFieldTemplate> fieldList;

    /**
     * Constructor.
     */
    WikiDimensionField() {}

    /**
     * Set field info
     */
    public void setFieldName(String name) {
        this.fieldName = name;
    }
    public void setFieldList(LinkedHashSet<WikiDimensionFieldTemplate> list) {
        this.fieldList = list;
    }

    /**
     * Get field info
     */
    public String getFieldName() {return this.fieldName;}
    public LinkedHashSet<WikiDimensionFieldTemplate> getFieldList() {
        return fieldList;
    }
}
