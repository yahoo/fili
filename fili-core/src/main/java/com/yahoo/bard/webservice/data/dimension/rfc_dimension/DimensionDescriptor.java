// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;

/**
 * Dimension descriptor is intended to be an extensible container for storing and serializing metadata about the
 * semantic contents of a dimension.
 */
public interface DimensionDescriptor {

    String DEFAULT_CATEGORY = "General";

    /**
     * Getter for description.
     *
     * @see {@link Dimension#getDescription()}
     *
     * @return description
     */
    String getDescription();

    /**
     * Get the category of the dimension.
     *
     * @see {@link Dimension#getCategory()}
     *
     * @return category
     */
    String getCategory();

    /**
     * Get the long name of the dimension.
     *
     * @see {@link Dimension#getLongName()}
     *
     * @return long name
     */
    String getLongName();

}
