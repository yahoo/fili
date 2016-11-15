// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.druid.model.filter.AndFilter;
import com.yahoo.bard.webservice.druid.model.filter.Filter;

import java.util.Arrays;

/**
 * A filter representing AND.
 */
public class AndFilterNode extends FilterNode {
    protected final FilterNode left;
    protected final FilterNode right;

    /**
     * Construct a new AndFilter.
     *
     * @param left left operand
     * @param right right operand
     */
    public AndFilterNode(FilterNode left, FilterNode right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Filter buildFilter() {
        return new AndFilter(Arrays.asList(left.buildFilter(), right.buildFilter()));
    }
}
