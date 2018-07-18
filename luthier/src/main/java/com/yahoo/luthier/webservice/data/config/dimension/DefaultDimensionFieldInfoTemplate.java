// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.List;
import java.util.Objects;

/**
 * Dimension field set template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "name": "ID",
 *          "description": "Dimension ID",
 *          "tags": [
 *              "primaryKey"
 *          ]
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultDimensionFieldInfoTemplate implements DimensionFieldInfoTemplate {

    private final String name;
    private final String description;
    private final List<String> tags;

    /**
     * Constructor used by json parser.
     *
     * @param name json property name
     * @param description json property description
     * @param tags json property tags
     */
    @JsonCreator
    public DefaultDimensionFieldInfoTemplate(
            @NotNull @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("tags") List<String> tags
    ) {
        this.name = EnumUtils.camelCase(name);
        this.description = (Objects.isNull(description) ? "" : description);
        this.tags = tags;
    }

    @Override
    public String getFieldName() {
        return this.name;
    }

    @Override
    public String getFieldDescription() {
        return this.description;
    }

    /**
     * Get dimensions tags.
     *
     * @return a set of dimension tags
     */
    public List<String> getTags() {
        return this.tags;
    }

    @Override
    public DimensionField build() {
        return new DimensionField() {
            @Override
            public String getName() {
                return getFieldName();
            }
            @Override
            public String getDescription() {
                return getFieldDescription();
            }
            @Override
            public String toString() {
                return this.getName();
            }
        };
    }
}
