// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.LinkedHashSet;

/**
 * A schema describing the fields (columns) for a dimension.
 *
 */
public interface DomainSchema {

    /**
     * Getter for dimension fields.
     *
     * @see {@link Dimension#getDimensionFields()}
     *
     * @return Set of dimension fields
     */
    LinkedHashSet<DimensionField> getDimensionFields();

    /**
     * Find dimension field by name.
     *
     * @see {@link Dimension#getFieldByName(String)}
     *
     * @param name  field name
     *
     * @return DimensionField
     *
     * @throws IllegalArgumentException if this dimension does not have a field with the specified name
     */
    DimensionField getFieldByName(String name);

    /**
     *
     * @see {@link Dimension#getKey()}
     * Get primary key field for this dimension.
     *
     * @return primary key field
     */
    DimensionField getKey();


}
