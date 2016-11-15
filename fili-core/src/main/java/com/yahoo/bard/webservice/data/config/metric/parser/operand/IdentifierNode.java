// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;

/**
 * An IdentifierNode is special because it can be treated as a MetricNode *or* a DimensionNode.
 */
public class IdentifierNode implements Operand, MetricNode, DimensionNode {

    protected final String name;
    protected final DimensionDictionary dimensionDictionary;
    protected final MetricDictionary metricDictionary;

    /**
     * Create a new node wrapping a name.
     *
     * @param name the dimension or metric name
     * @param dimensionDictionary the dimension dictionary
     * @param metricDictionary the metric dictionary
     */
    public IdentifierNode(String name, DimensionDictionary dimensionDictionary, MetricDictionary metricDictionary) {
        this.name = name;
        this.dimensionDictionary = dimensionDictionary;
        this.metricDictionary = metricDictionary;
    }

    // Doesn't actually use the metricName parameter - may not be doing what I intend
    @Override
    public LogicalMetric make(String metricName, MetricDictionary dictionary) {
        return metricDictionary.get(name);
    }

    // FIXME:
    // This used to use findByDruidName(name), but that method was removed.
    // I'm not entirely sure that this version actually works. You probably need to filter by the API name, though.
    @Override
    public Dimension getDimension() {
        return dimensionDictionary.findByApiName(name);
    }
}
