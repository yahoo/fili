// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.yahoo.bard.webservice.data.dimension.*;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.yahoo.bard.webservice.util.EnumUtils;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Wiki dimension template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "apiName": "REGION_ISO_CODE",
 *          "longName": "wiki regionIsoCode",
 *          "description": "Iso Code of the region to which the wiki page belongs",
 *          "fields": "default"
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiDimensionTemplate implements DimensionConfigAPI {

    @JsonProperty("apiName")
    private final String apiName;

    @JsonProperty("description")
    private final String description;

    @JsonProperty("longName")
    private final String longName;

    @JsonProperty("category")
    private final String category;

    @JsonDeserialize(using = DimensionFieldDeserializer.class)
    @JsonProperty("fields")
    private WikiDimensionFieldConfigTemplate fields;

    /**
     * Constructor used by json parser.
     *
     * @param apiName     json property apiName
     * @param description json property description
     * @param longName    json property longName
     * @param category    json property category
     * @param fields      json property fields deserialize by DimensionFieldDeserializer
     */
    @JsonCreator
    public WikiDimensionTemplate(
            @NotNull @JsonProperty("apiName") String apiName,
            @JsonProperty("description") String description,
            @JsonProperty("longName") String longName,
            @JsonProperty("category") String category,
            @JsonProperty("fields") WikiDimensionFieldConfigTemplate fields
    ) {
        this.apiName = EnumUtils.camelCase(apiName);
        this.description = (Objects.isNull(description) ? "" : description);
        this.longName = (Objects.isNull(longName) ? EnumUtils.camelCase(apiName) : longName);
        this.category = (Objects.isNull(category) ? Dimension.DEFAULT_CATEGORY : category);
        this.fields = (Objects.isNull(fields) ? null : fields);
    }

    /**
     * Get dimensions info.
     */
    @Override
    public String getApiName() {
        return this.apiName;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getLongName() {
        return this.longName;
    }

    @Override
    public String getCategory() {
        return this.category;
    }

    @Override
    public String asName() {
        return this.apiName;
    }

    @Override
    public String toString() {
        return this.asName();
    }

    @Override
    public LinkedHashSet<DimensionField> getFields(HashMap<String,
            LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldDictionary) {
        resolveFields(fieldDictionary);
        return new LinkedHashSet<>(this.fields.getFieldList());
    }

    /**
     * Parse fields info based on dimension's "field" key word.
     * <p>
     * If "field list is no empty", use fields in field list
     * If "no field list" and "field has a name", map name in fieldSetInfo to get a field list
     * If "no field list" and "no field name", use default field list in fieldSetInfo
     *
     * @param fieldDictionary a map from fieldset's name to fieldset
     */
    private void resolveFields(HashMap<String,
            LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldDictionary) {

        // if specific fields
        if (this.fields != null && this.fields.getFieldList() != null) {
            this.fields.setFieldName("Specific");
        }

        // default fields
        else if (this.fields == null || this.fields.getFieldName() == null && this.fields.getFieldList() == null) {
            this.fields = new WikiDimensionFieldConfigTemplate();
            this.fields.setFieldName("Default");
            this.fields.setFieldList(fieldDictionary.get("default"));
        }

        // named fields
        else if (fieldDictionary.containsKey(this.fields.getFieldName())) {
            this.fields.setFieldList(fieldDictionary.get(this.fields.getFieldName()));
        }

        // others -> default
        else {
            this.fields = new WikiDimensionFieldConfigTemplate();
            this.fields.setFieldName("Default");
            this.fields.setFieldList(fieldDictionary.get("default"));
        }
    }
}
