// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operator;


import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.FunctionMetricNode;
import com.yahoo.bard.webservice.data.config.metric.parser.operand.Operand;

import java.util.List;

/**
 * A function operator.
 *
 * Are functions really operators? Mostly. And, it's a cleaner than
 * implementing the alternative.
 */
public class FunctionOperator implements Operator {

    protected final int numOperands;
    protected final MetricMaker maker;

    /**
     * Construct a function operator for the given metric maker.
     *
     * @param maker metric maker to construct function from
     * @param numOperands the number of operands the function accepts
     */
    public FunctionOperator(MetricMaker maker, int numOperands) {
        this.numOperands = numOperands;
        this.maker = maker;
    }

    @Override
    public Precedence getPrecedence() {
        return Precedence.FUNCTION;
    }

    @Override
    public int getNumOperands() {
        return numOperands;
    }

    @Override
    public Operand build(List<Operand> operands) throws ParsingException {
        return new FunctionMetricNode(maker, operands);
    }
}
