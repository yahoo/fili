// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.druid.model.filter.OrFilter;

import java.util.Arrays;

/**
 * A filter representing OR.
 *
 * FIXME: Could support combining nested statements into a single filter so that:
 *
 *     a || b || c
 *
 *  produces one node, not two.
 */
public class OrFilterNode extends FilterNode {

    protected final FilterNode left;
    protected final FilterNode right;

    /**
     * Construct a new OrFilter.
     *
     * @param left left operand
     * @param right right operand
     */
    public OrFilterNode(FilterNode left, FilterNode right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Filter buildFilter() {
        return new OrFilter(Arrays.asList(left.buildFilter(), right.buildFilter()));
    }
}
