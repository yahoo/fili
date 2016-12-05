// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.dimension.Dimension;

/**
 * Node that contains a dimension name.
 */
public interface DimensionNode extends Operand {

    /**
     * Get the dimension.
     * @return the dimension
     */
    Dimension getDimension();
}
