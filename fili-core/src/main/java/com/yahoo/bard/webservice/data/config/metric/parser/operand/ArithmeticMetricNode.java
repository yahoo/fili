// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.config.metric.makers.ArithmeticMaker;
import com.yahoo.bard.webservice.data.config.metric.makers.MetricMaker;
import com.yahoo.bard.webservice.data.config.metric.parser.MetricParser;
import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.mappers.NoOpResultSetMapper;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;

import java.util.Arrays;

/**
 * A binary arithmetic node.
 */
public class ArithmeticMetricNode implements MetricNode {

    protected final ArithmeticPostAggregation.ArithmeticPostAggregationFunction func;
    protected final MetricNode left;
    protected final MetricNode right;

    /**
     * Create an arithmetic metric node.
     *
     * @param func the function to apply
     * @param left the left operand
     * @param right the right operand
     */
    public ArithmeticMetricNode(
            ArithmeticPostAggregation.ArithmeticPostAggregationFunction func,
            MetricNode left,
            MetricNode right
    ) {
        this.func = func;
        this.left = left;
        this.right = right;
    }

    @Override
    public LogicalMetric make(String metricName, MetricDictionary dictionary) throws ParsingException {
        LogicalMetric l = left.make(MetricParser.getNewName(), dictionary);
        LogicalMetric r = right.make(MetricParser.getNewName(), dictionary);
        MetricMaker maker = new ArithmeticMaker(dictionary, func, new NoOpResultSetMapper());

        LogicalMetric result = maker.make(metricName, Arrays.asList(l.getName(), r.getName()));
        dictionary.add(result);
        return result;
    }
}
