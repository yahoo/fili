// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;

import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.FilterNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.FilteredAggMetricNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.MetricNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand;

import java.util.List;

/**
 * Filter operator (|).
 */
public class FilterOperator implements Operator {

    @Override
    public Precedence getPrecedence() {
        return Precedence.FILTER;
    }

    @Override
    public int getNumOperands() {
        return 2;
    }

    @Override
    public Operand build(List<Operand> operands) throws ParsingException {
        MetricNode left = operands.get(0).getMetricNode();
        FilterNode right = operands.get(1).getFilterNode();

        return new FilteredAggMetricNode(left, right);
    }
}
