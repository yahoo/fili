package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.yahoo.bard.webservice.data.dimension.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.bard.webservice.util.Utils;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiDimensionTemplate extends Template implements DimensionConfigAPI {

    @JsonProperty("apiName")
    private String apiName;

    @JsonProperty("description")
    private String description;

    @JsonProperty("longName")
    private String longName;

    @JsonProperty("category")
    private String category;

    @JsonDeserialize(using = DimensionFieldDeserializer.class)
    private WikiDimensionField fields;

    /**
     * Constructor.
     */
    public WikiDimensionTemplate() { }

    /**
     * Set dimensions info.
     */
    @Override
    public void setApiName(String apiName) {
        this.apiName = apiName;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public void setLongName(String longName) {
        this.longName = longName;
    }

    @Override
    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public void setFields(WikiDimensionField fields) {this.fields = fields;}

    /**
     * Get dimensions info.
     */
    @Override
    public String getApiName() {
        return apiName;
    }

    @Override
    public String getDescription() {
        if (Objects.isNull(description)) {
            return "";
        }
        return description;
    }

    @Override
    public String getLongName() {
        if (Objects.isNull(longName)) {
            return getApiName();
        }
        return longName;
    }

    @Override
    public String getCategory() {
        if (Objects.isNull(category)) {
            return Dimension.DEFAULT_CATEGORY;
        }
        return category;
    }

    @Override
    public String asName() {
        return EnumUtils.camelCase(this.apiName);
    }

    @Override
    public WikiDimensionField getFields() {
        return this.fields;
    }

    @Override
    public String toString() {
        return this.asName();
    }

    /**
     * Parse fields info.
     */
    public LinkedHashSet<DimensionField> resolveFields(HashMap<String, LinkedHashSet<WikiDimensionFieldTemplate>> fieldSet) {

        // specific fields
        if (this.fields != null && this.fields.getFieldList() != null) {
            this.fields.setFieldName("Specific");
        }

        // default fields
        else if (this.fields == null || (this.fields.getFieldName() == null && this.fields.getFieldList() == null)) {
            this.fields = new WikiDimensionField();
            this.fields.setFieldName("Default");
            this.fields.setFieldList(fieldSet.get("default"));
        }

        // named fields
        else if (fieldSet.containsKey(this.fields.getFieldName())) {
            this.fields.setFieldList(fieldSet.get(this.fields.getFieldName()));
        }

        // others -> default
        else {
            this.fields = new WikiDimensionField();
            this.fields.setFieldName("Default");
            this.fields.setFieldList(fieldSet.get("default"));
        }

        return new LinkedHashSet<>(this.fields.getFieldList());

    }

}
