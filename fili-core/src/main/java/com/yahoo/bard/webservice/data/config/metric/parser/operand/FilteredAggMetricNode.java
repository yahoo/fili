// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.data.config.metric.parser.operand;

import com.yahoo.bard.webservice.data.config.metric.makers.FilteredAggregationMaker;
import com.yahoo.bard.webservice.data.config.metric.parser.MetricParser;
import com.yahoo.bard.webservice.data.config.metric.parser.ParsingException;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.druid.model.aggregation.Aggregation;

import java.util.Collections;
import java.util.Set;

/**
 * A filtered aggregation.
 */
public class FilteredAggMetricNode implements MetricNode {

    protected final IdentifierNode left;
    protected final FilterNode right;

    /**
     * Create a new filtered aggregation metric node.
     *
     * @param left the left operand, an identifier node
     * @param right the right operand, a filter node
     */
    public FilteredAggMetricNode(Operand left, Operand right) {
        this.left = left.getIdentifierNode();
        this.right = right.getFilterNode();
    }


    @Override
    public LogicalMetric make(String metricName, MetricDictionary dictionary) throws ParsingException {

        // FIXME: This seems...wrong? What if there is more than one aggregation in the left node?
        LogicalMetric leftNode = left.getMetricNode().make(MetricParser.getNewName(), dictionary);
        Set<Aggregation> aggregations = leftNode.getTemplateDruidQuery().getAggregations();
        Aggregation aggregation;
        if (aggregations != null && aggregations.size() >= 1) {
            aggregation = aggregations.iterator().next(); // Java, why is there no "Give me some element from this set"?
        } else {
            throw new RuntimeException("Could not find an aggregation");
        }
        FilteredAggregationMaker
                filteredAggregationMaker = new FilteredAggregationMaker(dictionary, aggregation, right.buildFilter());

        LogicalMetric metric = filteredAggregationMaker.make(metricName, Collections.emptyList());
        dictionary.put(metricName, metric);
        // FIXME: Dependant metrics? I *think* we don't need any because we are already wrapping the other query.
        return filteredAggregationMaker.make(metricName, Collections.emptyList());
    }
}
