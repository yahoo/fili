// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ColumnMapper;
import com.yahoo.bard.webservice.data.metric.mappers.SketchRoundUpMapper;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Metric maker for performing binary arithmetic operations on metrics.
 */
public class ArithmeticMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 2;

    private static final Logger LOG = LoggerFactory.getLogger(ArithmeticMaker.class);

    final ArithmeticPostAggregationFunction function;

    final ColumnMapper resultSetMapper;

    /**
     * Build a fully specified ArithmeticMaker.
     *
     * @param metricDictionary  The dictionary used to resolve dependent metrics when building the LogicalMetric
     * @param function  The arithmetic operation performed by the LogicalMetrics constructed by this maker
     * @param resultSetMapper  The function to be applied to the result that is returned by the query
    that is built from the LogicalMetric which is built by this maker.
     */
    public ArithmeticMaker(
            MetricDictionary metricDictionary,
            ArithmeticPostAggregationFunction function,
            ColumnMapper resultSetMapper
    ) {
        super(metricDictionary);
        this.function = function;
        this.resultSetMapper = resultSetMapper;
    }

    /**
     * Build an ArithmeticMaker with SketchRoundUpMapper as the ResultSetMapper for building the LogicalMetric.
     *
     * @param metricDictionary  The dictionary used to resolve dependent metrics when building the LogicalMetric
     * @param function  The arithmetic operation performed by the LogicalMetrics constructed by this maker
     */
    public ArithmeticMaker(MetricDictionary metricDictionary, ArithmeticPostAggregationFunction function) {
        // TODO: Deprecate me, mappers should always be specified at creation time, not implicitly
        this(metricDictionary, function, new SketchRoundUpMapper());
    }

    @Override
    protected LogicalMetric makeInner(String metricName, List<String> dependentMetrics) {
        TemplateDruidQuery mergedQuery = getMergedQuery(dependentMetrics);

        // Get the ArithmeticPostAggregation operands from the dependent metrics
        List<PostAggregation> operands = dependentMetrics.stream()
                .map(this::getNumericField)
                .collect(Collectors.toList());

        // Create the ArithmeticPostAggregation
        PostAggregation resultPostAgg = new ArithmeticPostAggregation(metricName, function, operands);
        Set<PostAggregation> postAggs = Collections.singleton(resultPostAgg);

        TemplateDruidQuery query = new TemplateDruidQuery(
                mergedQuery.getAggregations(),
                postAggs,
                mergedQuery.getInnerQuery(),
                mergedQuery.getTimeGrain()
        );

        // Note: We need to pass everything through ColumnMapper
        // We need to refactor this to be a list.
        return new LogicalMetric(query, resultSetMapper.withColumnName(metricName), metricName);
    }

    @Override
    protected int getDependentMetricsRequired() {
        return DEPENDENT_METRICS_REQUIRED;
    }

    /**
     * Unlike most other metrics, Arithmetic metrics require at least DEPENDENT_METRICS_REQUIRED dependent metrics,
     * rather than exactly DEPENDENT_METRICS_REQUIRED.
     *
     * @param dictionaryName  Name of the metric being made
     * @param dependentMetrics  List of dependent metrics needed to make the metric
     */
    @Override
    protected void assertRequiredDependentMetricCount(String dictionaryName, List<String> dependentMetrics) {
        int minimalCount = getDependentMetricsRequired();
        int actualCount = dependentMetrics.size();
        if (actualCount < minimalCount) {
            String message = String.format(
                    "%s got %d of at least %d dependent metrics",
                    dictionaryName,
                    actualCount,
                    minimalCount
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }
}
