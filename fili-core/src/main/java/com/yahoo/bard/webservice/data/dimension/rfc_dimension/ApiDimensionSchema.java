// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;

import java.util.LinkedHashSet;

/**
 * A schema describing the fields (columns) for a dimension.
 *
 * In addition to the components of the DomainSchema it reflects the default response columns in queries.
 *
 */
public interface ApiDimensionSchema extends DomainSchema {

    /**
     * Getter for default dimension fields.
     *
     * @see {@link Dimension#getDefaultDimensionFields()}
     *
     * @return Set of dimension fields
     */
    LinkedHashSet<DimensionField> getDefaultDimensionFields();
}
