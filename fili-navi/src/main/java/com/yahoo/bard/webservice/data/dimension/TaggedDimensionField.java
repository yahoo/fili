// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import java.util.Set;

/**
 * Dimension field with tags.
 */
public interface TaggedDimensionField extends DimensionField {

    /**
     * Get a list of tags associated to the current field.
     *
     * @return a list of tags
     */
    Set<? extends Tag> getTags();
}
