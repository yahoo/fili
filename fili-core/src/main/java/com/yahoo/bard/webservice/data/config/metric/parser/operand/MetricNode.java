// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

/**
 * Tree node that handles naming metrics and building dependant metrics.
 */
public interface MetricNode extends Operand {

    /**
     * Make and add to the dictionary by assigned name.
     *
     * @param metricName name of the metric to assign
     * @param dictionary dictionary to add the metric to
     * @return the LogicalMetric representing this metric
     * @throws ParsingException when an error building the metric occurs
     */
    LogicalMetric make(String metricName, MetricDictionary dictionary) throws ParsingException;
}
