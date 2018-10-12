// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.datatype;

/**
 * A datatype for a dimension.
 */
public interface DimensionDatatype {

    /**
     * Gets the name of the datatype as a string.
     * @return the name
     */
    String getName();

    /**
     * Checks if the current datatype is either equivalent to or a subtype of another datatype.
     * For example, a "money" datatype could be a subtype of a "number" datatype. This can allow "number" comparisons
     * to be applied to dimensions of the "money" type, but "money" comparisons to not be applied to "number" datatypes.
     *
     * @param otherType  The datatype to compare to
     * @return  whether or not this object is equivalent to or a subtype of otherType
     */
    boolean isA(DimensionDatatype otherType);
}
