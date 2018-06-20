// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.yahoo.bard.webservice.data.dimension.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Wiki dimension template.
 *
 * An example:
 *
 *     {
 *       "apiName": "REGION_ISO_CODE",
 *       "longName": "wiki regionIsoCode",
 *       "description": "Iso Code of the region to which the wiki page belongs",
 *       "fields": "default"
 *     }
 *
 */
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
    @JsonProperty("fields")
    private WikiDimensionFieldConfigTemplate fields;

    /**
     * Constructor used by json parser.
     *
     * @param apiName     json property apiName
     * @param description json property description
     * @param longName    json property longName
     * @param category    json property categor
     * @param fields      json property fields deserialize by DimensionFieldDeserializer
     */
    @JsonCreator
    public WikiDimensionTemplate(
            @JsonProperty("apiName") String apiName,
            @JsonProperty("description") String description,
            @JsonProperty("longName") String longName,
            @JsonProperty("category") String category,
            @JsonProperty("fields") WikiDimensionFieldConfigTemplate fields
    ) {
        setApiName(apiName);
        setDescription(description);
        setLongName(longName);
        setCategory(category);
        setFields(fields);
    }

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
    public void setFields(WikiDimensionFieldConfigTemplate fields) {
        this.fields = fields;
    }

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
    public WikiDimensionFieldConfigTemplate getFields() {
        return this.fields;
    }

    @Override
    public String toString() {
        return this.asName();
    }

    /**
     * Parse fields info based on dimension's "field" key word.
     * <p>
     * If "field list is no empty", use fields in field list
     * If "no field list" and "field has a name", map name in fieldSetInfo to get a field list
     * If "no field list" and "no field name", use default field list in fieldSetInfo
     *
     * @param fieldSetInfo a map from fieldset's name to fieldset
     * @return a set of dimension field for this dimension
     */
    public LinkedHashSet<DimensionField> resolveFields(HashMap<String,
            LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldSetInfo) {

        // if specific fields
        if (this.fields != null && this.fields.getFieldList() != null) {
            this.fields.setFieldName("Specific");
        }

        // default fields
        else if (this.fields == null || this.fields.getFieldName() == null && this.fields.getFieldList() == null) {
            this.fields = new WikiDimensionFieldConfigTemplate();
            this.fields.setFieldName("Default");
            this.fields.setFieldList(fieldSetInfo.get("default"));
        }

        // named fields
        else if (fieldSetInfo.containsKey(this.fields.getFieldName())) {
            this.fields.setFieldList(fieldSetInfo.get(this.fields.getFieldName()));
        }

        // others -> default
        else {
            this.fields = new WikiDimensionFieldConfigTemplate();
            this.fields.setFieldName("Default");
            this.fields.setFieldList(fieldSetInfo.get("default"));
        }

        return new LinkedHashSet<>(this.fields.getFieldList());
    }
}
