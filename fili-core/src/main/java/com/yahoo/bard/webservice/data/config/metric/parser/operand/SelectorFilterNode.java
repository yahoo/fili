// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.SelectorFilter;

/**
 * A filter representing SELECTOR.
 */
public class SelectorFilterNode extends FilterNode {
    protected final DimensionNode dimension;
    protected final ConstantMetricNode value;

    /**
     * Construct a new selector filter.
     * @param dimension dimension to filter no
     * @param value dimension value to filter by
     */
    public SelectorFilterNode(DimensionNode dimension, ConstantMetricNode value) {
        this.dimension = dimension;
        this.value = value;
    }

    @Override
    public Filter buildFilter() {
        return new SelectorFilter(dimension.getDimension(), value.getValue());
    }
}
