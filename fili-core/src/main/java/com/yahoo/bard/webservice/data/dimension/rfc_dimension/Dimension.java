// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension;

import com.yahoo.bard.webservice.druid.serializers.DimensionToDefaultDimensionSpec;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Dimension interface.
 * <p>
 * ApiName must be unique to corresponding domain.
 * <p>
 * NOTE: To override the default serialization, use the @JsonSerialize on the implementing class.
 *       Using @JsonSerialize with no parameters will provide default Jackson behavior (so things such
 *       as @JsonValue will work properly) or else you can provide your own custom serializers using the
 *       same approach.
 */
@JsonSerialize(using = DimensionToDefaultDimensionSpec.class)
public interface Dimension {

    /**
     * Getter for api name.
     *
     * @return apiName
     */
    String getApiName();

    /**
     * Encapsulates methods returning {@link com.yahoo.bard.webservice.data.dimension.DimensionField})"
     *
     * @return  The dimension field schema for this dimension.
     */
    ApiDimensionSchema getSchema();

    /**
     * Encapsulates primarily metadata fields.
     *
     * @return The dimension schema class
     */
    DimensionDescriptor getDimensionDescriptor();

    /**
     * Encapsulates DimensionRows and cardinality
     *
     * @return The dimension row storage associated with this dimension.
     */
    IndexedDomain getDomain();

    /**
     * Return whether this dimension can be aggregated across.
     *
     * @return  true if this dimension is aggregatable
     */
    boolean isAggregatable();
}
