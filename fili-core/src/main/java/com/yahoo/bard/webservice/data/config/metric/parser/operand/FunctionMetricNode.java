// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.metric.parser.MetricParser;
import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.LinkedList;
import java.util.List;

/**
 * A function node (e.g. longSum())
 */
public class FunctionMetricNode implements MetricNode {

    protected final MetricMaker maker;
    protected final List<Operand> operands;

    /**
     * Create a new function metric node.
     *
     * @param maker the metric maker that implements this function
     * @param operands the operands passed to the function
     */
    public FunctionMetricNode(MetricMaker maker, List<Operand> operands) {
        this.maker = maker;
        this.operands = operands;
    }

    @Override
    public LogicalMetric make(String metricName, MetricDictionary dictionary) throws ParsingException {
        List<String> fields = new LinkedList<>();

        for (Operand operand : operands) {
            if (operand instanceof MetricNode) {
                LogicalMetric metric = operand.getMetricNode().make(MetricParser.getNewName(), dictionary);

                // FIXME: Not sure if all the logic here is right
                if (metric != null) {
                    fields.add(metric.getName());
                } else if (operand instanceof IdentifierNode) {
                    fields.add(((IdentifierNode) operand).name);
                } else {
                    throw new ParsingException("Unexpected node type: " + operand.toString());
                }
            } else {
                throw new ParsingException("Unexpected node type: " + operand.toString());
            }
        }
        LogicalMetric metric = maker.make(metricName, fields);
        dictionary.add(metric);
        return metric;
    }
}
