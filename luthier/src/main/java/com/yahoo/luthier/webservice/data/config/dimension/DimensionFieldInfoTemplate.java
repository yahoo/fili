// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionField;

/**
 * Dimension Field info Template.
 */
public interface DimensionFieldInfoTemplate {

    /**
     * Get dimensions field's name.
     *
     * @return dimensions field's name
     */
    String getFieldName();

    /**
     * Get dimensions field's description.
     *
     * @return dimensions field's description
     */
    String getFieldDescription();

    /**
     * Build a DimensionField instance.
     *
     * @return a DimensionField instance
     */
    DimensionField build();

}
