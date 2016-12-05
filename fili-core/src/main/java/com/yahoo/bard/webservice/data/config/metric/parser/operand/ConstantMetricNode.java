// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.config.metric.makers.ConstantMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

import java.util.Collections;

/**
 * Node that handles constants.
 */
public class ConstantMetricNode implements MetricNode {

    protected final String value;

    /**
     * Create a new ConstantMetricNode.
     *
     * @param value constant value
     */
    public ConstantMetricNode(String value) {
        this.value = value;
    }

    @Override
    public LogicalMetric make(String metricName, MetricDictionary dictionary) {
        LogicalMetric result = new ConstantMaker(dictionary).make(metricName, Collections.singletonList(value));
        dictionary.add(result);
        return result;
    }

    /**
     * Get the constant node's value.
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }
}
