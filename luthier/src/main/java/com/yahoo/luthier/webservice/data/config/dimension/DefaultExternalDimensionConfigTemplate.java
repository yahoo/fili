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
 *          "dimensions": [
 *              a list of dimensions deserialize by ikiDimensionTemplate
 *          ]
 *      }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DefaultExternalDimensionConfigTemplate implements ExternalDimensionConfigTemplate {

    private final Set<DimensionTemplate> dimensions;

    /**
     * Constructor used by json parser.
     *
     * @param dimensions json property dimensions
     */
    @JsonCreator
    public DefaultExternalDimensionConfigTemplate(
            @JsonProperty("dimensions") Set<DimensionTemplate> dimensions
    ) {
        this.dimensions = dimensions;
    }

    @Override
    public Set<DimensionTemplate> getDimensions() {
        return this.dimensions;
    }
}
