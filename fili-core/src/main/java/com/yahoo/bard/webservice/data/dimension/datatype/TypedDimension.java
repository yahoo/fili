// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.datatype;

import com.yahoo.bard.webservice.data.dimension.Dimension;

/**
 * A dimension that has a datatype.
 */
public interface TypedDimension extends Dimension {

    /**
     * Getter for the datatype of this dimension.
     * @return the datatype
     */
    DimensionDatatype getDatatype();
}
