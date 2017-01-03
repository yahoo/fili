// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchEstimatePostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggregation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Metric maker to support sketch set post aggregations.
 *
 * @deprecated  To consider the latest version of sketch Library.
 * This class is replaced by ThetaSketchSetOperationMaker
 */
@Deprecated
public class SketchSetOperationMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 2;

    private final SketchSetOperationPostAggFunction function;

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param function  The set operation to perform.
     */
    public SketchSetOperationMaker(MetricDictionary metrics, SketchSetOperationPostAggFunction function) {
        super(metrics);
        this.function = function;
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {

        TemplateDruidQuery mergedQuery = getMergedQuery(dependentMetrics);

        // Get the SketchSetOperationPostAggregation operands from the dependent metrics
        PostAggregation operandOne = getSketchField(dependentMetrics.get(0));
        PostAggregation operandTwo = getSketchField(dependentMetrics.get(1));

        // Create the SketchSetOperationPostAggregation
        SketchSetOperationPostAggregation setPostAggregation;
        setPostAggregation = new SketchSetOperationPostAggregation(
                metricName,
                function,
                Arrays.asList(operandOne, operandTwo)
        );

        PostAggregation estimate = new SketchEstimatePostAggregation(metricName, setPostAggregation);
        Set<PostAggregation> postAggs = Collections.singleton(estimate);

        TemplateDruidQuery query = new TemplateDruidQuery(
                mergedQuery.getAggregations(),
                postAggs,
                mergedQuery.getInnerQuery(),
                mergedQuery.getTimeGrain()
        );

        return new LogicalMetric(query, new SketchRoundUpMapper(metricName), metricName);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
