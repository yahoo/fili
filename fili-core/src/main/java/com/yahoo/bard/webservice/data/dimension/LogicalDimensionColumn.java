// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.rfc.data.dimension.DimensionColumn;

/**
 * A LogicalDimensionColumn defines a dimension column that is not tied to any particular physical table.
 *
 * @deprecated in favor of DimensionColumn which stores name as api name now
 */
@Deprecated
public class LogicalDimensionColumn extends DimensionColumn {
    /**
     * Constructor.
     *
     * @param dimension  The dimension to create a column for
     */
    public LogicalDimensionColumn(Dimension dimension) {
        super(dimension, dimension.getApiName());
    }
}
