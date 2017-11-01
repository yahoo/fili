// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.SketchSetOperationPostAggFunction;
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchEstimatePostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ThetaSketchSetOperationPostAggregation;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Metric maker to support sketch set post aggregations.
 */
public class ThetaSketchSetOperationMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 2;

    private final SketchSetOperationPostAggFunction function;

    /**
     * Constructor.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param function  The set operation to perform.
     */
    public ThetaSketchSetOperationMaker(MetricDictionary metrics, SketchSetOperationPostAggFunction function) {
        super(metrics);
        this.function = function;
    }

    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {

        TemplateDruidQuery mergedQuery = getMergedQuery(dependentMetrics);

        // Get the ThetaSketchSetOperationPostAggregation operands from the dependent metrics
        List<PostAggregation> sketchPostAggregations = dependentMetrics.stream()
                .map(this::getSketchField)
                .collect(Collectors.toList());

        // Create the ThetaSketchSetOperationPostAggregation
        String metricName = logicalMetricInfo.getName();
        ThetaSketchSetOperationPostAggregation setPostAggregation = new ThetaSketchSetOperationPostAggregation(
                metricName,
                function,
                sketchPostAggregations
        );

        PostAggregation estimate = new ThetaSketchEstimatePostAggregation(metricName, setPostAggregation);
        TemplateDruidQuery query = mergedQuery.withPostAggregations(Collections.singleton(estimate));
        return new LogicalMetric(query, new SketchRoundUpMapper(metricName), logicalMetricInfo);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }
}
