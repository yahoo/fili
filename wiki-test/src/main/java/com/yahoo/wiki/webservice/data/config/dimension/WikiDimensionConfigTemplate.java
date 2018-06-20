// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.wiki.webservice.data.config.Template;

import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * Wiki dimension config template.
 *
 * An example:
 *
 * {
 *   "fieldSets": {
 *   -> a list of fieldset deserialize by WikiDimensionFieldSetsTemplate
 *     },
 *   "dimensions": [
 *   -> a list of dimensions deserialize by ikiDimensionTemplate
 *     ]
 * }
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiDimensionConfigTemplate extends Template {

    @JsonProperty("fieldSets")
    private HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldSets;

    @JsonProperty("dimensions")
    private LinkedHashSet<WikiDimensionTemplate> dimensions;

    /**
     * Constructor used by json parser.
     *
     * @param fieldSets  json property fieldSets
     * @param dimensions json property dimensions
     */
    @JsonCreator
    public WikiDimensionConfigTemplate(
           @JsonProperty("fieldSets") HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldSets,
           @JsonProperty("dimensions") LinkedHashSet<WikiDimensionTemplate> dimensions
    ) {
        setFields(fieldSets);
        setDimensions(dimensions);
    }

    /**
     * Set field configuration info.
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
     * Get field configuration info.
     *
     * @return a map from fieldset name to fieldset
     */
    public HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> getFieldSets() {
        return this.fieldSets;
    }

    /**
     * Get dimensions configuration info.
     *
     * @return a set of dimensions
     */
    public LinkedHashSet<WikiDimensionTemplate> getDimensions() {
        return this.dimensions;
    }
}
