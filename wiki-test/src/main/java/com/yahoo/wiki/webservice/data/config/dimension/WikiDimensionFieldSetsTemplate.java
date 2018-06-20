// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.util.EnumUtils;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.LinkedList;

/**
 * Dimension field set template.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiDimensionFieldSetsTemplate extends Template implements DimensionField {

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("tags")
    private LinkedList<String> tags;

    /**
     * Constructor used by json parser.
     *
     * @param name        json property name
     * @param description json property description
     * @param tags        json property tags
     */
    @JsonCreator
    public WikiDimensionFieldSetsTemplate(
            @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("tags") LinkedList<String> tags
    ) {
        setName(name);
        setDescription(description);
        setTags(tags);
    }

    /**
     * Set dimensions name.
     *
     * @param name dimension name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Set dimensions description.
     *
     * @param description dimension description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Set dimensions tags.
     *
     * @param tags a set of dimension tags
     */
    public void setTags(LinkedList<String> tags) {
        if (tags != null) { this.tags = new LinkedList<>(tags); }
    }

    @Override
    public String getName() {
        return EnumUtils.camelCase(this.name);
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    /**
     * Get dimensions tags.
     *
     * @return a set of dimension tags
     */
    public LinkedList<String> getTags() {
        return this.tags;
    }

    @Override
    public String toString() {
        return EnumUtils.camelCase(this.getName());
    }
}
