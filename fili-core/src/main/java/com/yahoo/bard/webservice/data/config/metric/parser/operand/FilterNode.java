// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.druid.model.filter.Filter;

/**
 * FilterNode represents a Druid filter condition.
 *
 * Note: Rick suggests taking advantage of the existing implementation; see:
 * https://github.com/yahoo/fili/blob/master/fili-core/src/main/java/com/yahoo/bard/webservice/web/ApiFilter.java
 */
public abstract class FilterNode implements Operand {

    /**
     * Build a Bard filter object.
     * @return the filter
     */
    abstract Filter buildFilter();

    /**
     * Create a new FilterNode.
     *
     * FIXME: We probably can add better error messages when the left/right are the wrong types
     *
     * @param filterType the type of filter to create
     * @param left the left filter operand
     * @param right the right filter operand
     * @return a filter node
     * @throws ParsingException if an unsupported filter type is given
     */
    public static FilterNode create(Filter.DefaultFilterType filterType, Operand left, Operand right)
            throws ParsingException {
        switch (filterType) {
            case OR:
                return new OrFilterNode(left.getFilterNode(), right.getFilterNode());
            case AND:
                return new AndFilterNode(left.getFilterNode(), right.getFilterNode());
            case SELECTOR:
                return new SelectorFilterNode(left.getDimensionNode(), right.getConstantNode());
            default:
                throw new ParsingException("Could not handle filter type: " + filterType);
        }
    }
}
