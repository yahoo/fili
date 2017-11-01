// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.config.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ColumnMapper;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation;
import com.yahoo.bard.webservice.druid.model.postaggregation.ArithmeticPostAggregation.ArithmeticPostAggregationFunction;
import com.yahoo.bard.webservice.druid.model.postaggregation.PostAggregation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Metric maker for performing binary arithmetic operations on metrics.
 */
public class ArithmeticMaker extends MetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 2;

    private static final Logger LOG = LoggerFactory.getLogger(ArithmeticMaker.class);

    private final ArithmeticPostAggregationFunction function;

    private final Function<String, ResultSetMapper> resultSetMapperSupplier;

    /**
     * Constructor.
     *
     * @param metricDictionary  The dictionary used to resolve dependent metrics when building the LogicalMetric
     * @param function  The arithmetic operation performed by the LogicalMetrics constructed by this maker
     * @param resultSetMapperSupplier  A function that takes a metric column name and produces at build time, a result
     * set mapper.
     */
    public ArithmeticMaker(
            MetricDictionary metricDictionary,
            ArithmeticPostAggregationFunction function,
            Function<String, ResultSetMapper> resultSetMapperSupplier
    ) {
        super(metricDictionary);
        this.function = function;
        this.resultSetMapperSupplier = resultSetMapperSupplier;
    }

    /**
     * Constructor.
     *
     * @param metricDictionary  The dictionary used to resolve dependent metrics when building the LogicalMetric
     * @param function  The arithmetic operation performed by the LogicalMetrics constructed by this maker
     * @param resultSetMapper  The mapping function to be applied to the result that is returned by the query that is
     * built from the LogicalMetric which is built by this maker.
     *
     * @deprecated to override default mapping, use the Function constructor
     */
    @Deprecated
    public ArithmeticMaker(
            MetricDictionary metricDictionary,
            ArithmeticPostAggregationFunction function,
            ColumnMapper resultSetMapper
    ) {
        this(
                metricDictionary,
                function,
                (Function<String, ResultSetMapper>) (String name) -> (ResultSetMapper) resultSetMapper
        );
    }

    /**
     * Build an ArithmeticMaker with SketchRoundUpMapper as the ResultSetMapper for building the LogicalMetric.
     *
     * @param metricDictionary  The dictionary used to resolve dependent metrics when building the LogicalMetric
     * @param function  The arithmetic operation performed by the LogicalMetrics constructed by this maker
     */
    public ArithmeticMaker(MetricDictionary metricDictionary, ArithmeticPostAggregationFunction function) {
        this(
                metricDictionary,
                function,
                NO_OP_MAP_PROVIDER
        );
    }

    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        // Get the ArithmeticPostAggregation operands from the dependent metrics
        List<PostAggregation> operands = dependentMetrics.stream()
                .map(metrics::get)
                .map(LogicalMetric::getMetricField)
                .map(MetricMaker::getNumericField)
                .collect(Collectors.toList());

        // Create the ArithmeticPostAggregation
        Set<PostAggregation> postAggregations = Collections.singleton(new ArithmeticPostAggregation(
                logicalMetricInfo.getName(),
                function,
                operands
        ));

        TemplateDruidQuery query = getMergedQuery(dependentMetrics).withPostAggregations(postAggregations);
        return new LogicalMetric(
                query,
                resultSetMapperSupplier.apply(logicalMetricInfo.getName()),
                logicalMetricInfo
        );
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
