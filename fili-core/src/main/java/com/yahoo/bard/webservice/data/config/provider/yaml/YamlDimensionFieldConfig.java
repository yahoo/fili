// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider.yaml;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Pojo for deserializing yaml configuration of dimension fields.
 */
public class YamlDimensionFieldConfig implements DimensionField {

    protected String name;
    protected String description;
    protected Boolean defaultInclude;

    /**
     * Construct the dimension field configuration.
     *
     * @param description field description
     * @param defaultInclude true if field is included by default
     */
    @JsonCreator
    public YamlDimensionFieldConfig(
            @JsonProperty("description") String description,
            @JsonProperty("default_include") Boolean defaultInclude
    ) {
        this.description = description;
        if (defaultInclude != null) {
            this.defaultInclude = defaultInclude;
        } else {
            this.defaultInclude = false;
        }
    }

    /**
     * Set the dimension field name.
     *
     * Intended to be called by Jackson deserialization
     *
     * @param name the dimension field name
     */
    public void setName(String name) {
        this.name = name;

        // Default the description to the name
        if (this.description == null) {
            this.description = name;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Return true if the dimension field should be included in results by default.
     *
     * @return true if included by default, false otherwise
     */
    public Boolean includedByDefault() {
        return defaultInclude;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof YamlDimensionFieldConfig)) {
            return false;
        }

        YamlDimensionFieldConfig conf = (YamlDimensionFieldConfig) other;
        return Objects.equals(name, conf.name) &&
                Objects.equals(description, conf.description) &&
                Objects.equals(defaultInclude, conf.defaultInclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, defaultInclude);
    }
}
