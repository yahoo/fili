// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.data.metric.protocol.DefaultSystemMetricProtocols;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetric;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport;
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
public class ArithmeticMaker extends BaseProtocolMetricMaker {

    private static final int DEPENDENT_METRICS_REQUIRED = 2;

    private static final Logger LOG = LoggerFactory.getLogger(ArithmeticMaker.class);

    private final ArithmeticPostAggregationFunction function;

    private final Function<String, ResultSetMapper> resultSetMapperSupplier;

    private static final String RENAMED_ARITHMETIC_PREFIX = "__airthmetic_renamed_";

    /**
     * Constructor.
     *
     * @param metricDictionary  The dictionary used to resolve dependent metrics when building the LogicalMetric
     * @param function  The arithmetic operation performed by the LogicalMetrics constructed by this maker
     * @param resultSetMapperSupplier  A function that takes a metric column name and produces at build time, a
     * result set mapper.
     * @param baseProtocolSupport The protocols to support before removing any blacklisted dependencies
     *
     */
    public ArithmeticMaker(
            MetricDictionary metricDictionary,
            ArithmeticPostAggregationFunction function,
            Function<String, ResultSetMapper> resultSetMapperSupplier,
            ProtocolSupport baseProtocolSupport
    ) {
        super(metricDictionary, baseProtocolSupport);
        this.function = function;
        this.resultSetMapperSupplier = resultSetMapperSupplier;
    }

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
        this(
                metricDictionary,
                function,
                resultSetMapperSupplier,
                DefaultSystemMetricProtocols.getStandardProtocolSupport()
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
    protected String getRenamedMetricNameWithPrefix(String name) {
        return RENAMED_ARITHMETIC_PREFIX + name;
    }

    @Override
    public ResultSetMapper makeCalculation(LogicalMetricInfo logicalMetricInfo, List<LogicalMetric> dependentMetric) {
        return resultSetMapperSupplier.apply(logicalMetricInfo.getName());
    }

    @Override
    public TemplateDruidQuery makePartialQuery(
            LogicalMetricInfo logicalMetricInfo,
            List<LogicalMetric> dependentMetrics
    ) {
        List<PostAggregation> operands = dependentMetrics.stream()
                .map(LogicalMetric::getMetricField)
                .map(MetricMaker::getNumericField)
                .collect(Collectors.toList());

        // Create the ArithmeticPostAggregation
        Set<PostAggregation> postAggregations = Collections.singleton(new ArithmeticPostAggregation(
                logicalMetricInfo.getName(),
                function,
                operands
        ));
        return getMergedQuery(dependentMetrics).withPostAggregations(postAggregations);
    }

    @Override
    public ProtocolSupport makeProtocolSupport(
            LogicalMetricInfo logicalMetricInfo,
            List<LogicalMetric> dependentMetrics
    ) {
        List<ProtocolSupport> supportsOfDependents =
                dependentMetrics.stream()
                        .filter(metric -> metric instanceof ProtocolMetric)
                        .map(metric -> (ProtocolMetric) metric)
                        .map(ProtocolMetric::getProtocolSupport)
                        .collect(Collectors.toList());

        // Any blacklisted protocols in dependencies should roll upward
        return DefaultSystemMetricProtocols.getStandardProtocolSupport().mergeBlacklists(supportsOfDependents);
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
