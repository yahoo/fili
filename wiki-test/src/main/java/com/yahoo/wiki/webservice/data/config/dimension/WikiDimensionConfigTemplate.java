// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * Wiki dimension config template.
 */
public class WikiDimensionConfigTemplate extends Template {

    @JsonProperty("fieldSets")
    private HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldSets;

    @JsonProperty("dimensions")
    private LinkedHashSet<WikiDimensionTemplate> dimensions;

    /**
     * Constructor.
     */
    public WikiDimensionConfigTemplate() {
    }

    /**
     * Set dimensions configuration info.
     *
     * @param fieldSets a map from fieldset name to fieldset
     */
    public void setFields(HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldSets) {
        this.fieldSets = fieldSets;
    }

    /**
     * Set dimensions info.
     *
     * @param dimensions a set of dimensions
     */
    public void setDimensions(LinkedHashSet<WikiDimensionTemplate> dimensions) {
        this.dimensions = dimensions;
    }

    /**
     * Get dimensions configuration info.
     *
     * @return a map from fieldset name to fieldset
     */
    public HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> getFieldSets() {
        return this.fieldSets;
    }

    public LinkedHashSet<WikiDimensionTemplate> getDimensions() {
        return this.dimensions;
    }
}
