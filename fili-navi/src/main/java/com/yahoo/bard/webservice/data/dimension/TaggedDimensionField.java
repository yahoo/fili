// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Set;

/**
 * Interface for Dimension fields that expects unique tags to be attached to it to add expressiveness.
 */
@JsonPropertyOrder({"name", "description", "tags"})
public interface TaggedDimensionField extends DimensionField {

    /**
     * Get a set of tags associated to the current field.
     *
     * @return a list of tags
     */
    Set<? extends Tag> getTags();
}
