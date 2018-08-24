// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

import com.google.common.collect.ImmutableList;
import com.yahoo.bard.webservice.data.dimension.Tag;
import com.yahoo.bard.webservice.data.dimension.TaggedDimensionField;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
public class DefaultDimensionFieldInfoTemplate implements DimensionFieldInfoTemplate, TaggedDimensionField {

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
        this.tags = (Objects.isNull(tags) ? Collections.emptyList() : ImmutableList.copyOf(tags));
    }

    @Override
    public String getFieldName() {
        return this.name;
    }

    @Override
    public String getFieldDescription() {
        return this.description;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public Set<? extends Tag> getTags() {
        return this.tags.stream().map(tag -> (Tag) () -> (tag)).collect(Collectors.toSet());
    }

    @Override
    public TaggedDimensionField build() {
        return this;
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
