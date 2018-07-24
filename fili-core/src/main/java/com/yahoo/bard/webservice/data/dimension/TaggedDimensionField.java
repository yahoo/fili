package com.yahoo.bard.webservice.data.dimension;

import java.util.Set;

/**
 * Interface for Dimension fields that expects unique tags to be attached to it to add expressiveness.
 */
public interface TaggedDimensionField extends DimensionField {

    /**
     * Get a set of tags associated to the current field.
     *
     * @return a list of tags
     */
    Set<? extends Tag> getTags();
}
