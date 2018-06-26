// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * Wiki dimension config template.
 * <p>
 * An example:
 * <p>
 *      {
 *          "fieldSets": {
 *              a list of fieldset deserialize by WikiDimensionFieldSetsTemplate
 *          },
 *          "dimensions": [
 *              a list of dimensions deserialize by ikiDimensionTemplate
 *          ]
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikiDimensionConfigTemplate {

    private final HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldDictionary;
    private final LinkedHashSet<WikiDimensionTemplate> dimensions;

    /**
     * Constructor used by json parser.
     *
     * @param fieldDictionary  json property fieldSets
     * @param dimensions json property dimensions
     */
    @JsonCreator
    public WikiDimensionConfigTemplate(
            @JsonProperty("fieldSets") HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> fieldDictionary,
            @JsonProperty("dimensions") LinkedHashSet<WikiDimensionTemplate> dimensions
    ) {
        this.fieldDictionary = (Objects.isNull(fieldDictionary) ? null : new HashMap<>(fieldDictionary));
        this.dimensions = (Objects.isNull(dimensions) ? null : new LinkedHashSet<>(dimensions));
    }

    /**
     * Get field configuration info.
     *
     * @return a map from fieldset name to fieldset
     */
    public HashMap<String, LinkedHashSet<WikiDimensionFieldSetsTemplate>> getFieldSets() {
        return this.fieldDictionary;
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
