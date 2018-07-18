// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

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
public class DefaultExternalDimensionConfigTemplate implements ExternalDimensionConfigTemplate {

    private final Map<String, LinkedHashSet<DimensionFieldInfoTemplate>> fieldDictionary;
    private final Set<DimensionTemplate> dimensions;

    /**
     * Constructor used by json parser.
     *
     * @param fieldDictionary json property fieldSets
     * @param dimensions json property dimensions
     */
    @JsonCreator
    public DefaultExternalDimensionConfigTemplate(
            @JsonProperty("fieldSets") Map<String, LinkedHashSet<DimensionFieldInfoTemplate>> fieldDictionary,
            @JsonProperty("dimensions") Set<DimensionTemplate> dimensions
    ) {
        this.fieldDictionary = fieldDictionary;
        this.dimensions = dimensions;
    }

    @Override
    public Map<String, LinkedHashSet<DimensionFieldInfoTemplate>> getFieldSets() {
        return this.fieldDictionary;
    }

    @Override
    public Set<DimensionTemplate> getDimensions() {
        return this.dimensions;
    }
}
