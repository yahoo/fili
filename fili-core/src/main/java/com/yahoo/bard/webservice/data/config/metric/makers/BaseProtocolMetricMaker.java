// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.data.metric.protocol.DefaultSystemMetricProtocols;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetricImpl;

import java.util.List;

/**
 * Protocol Metrics can be transformed by accepting parameters for a protocol they support.
 *
 * A protocol in this case is a category of contract of transformation.
 */
public abstract class BaseProtocolMetricMaker extends MetricMaker implements MakeFromMetrics {

    /**
     * The default protocol support for metrics for derived metric makers.
     */
    protected final ProtocolSupport baseProtocolSupport;

    /**
     * Construct a fully specified MetricMaker.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     */
    public BaseProtocolMetricMaker(MetricDictionary metrics) {
        this(metrics, DefaultSystemMetricProtocols.getStandardProtocolSupport());
    }

    /**
     * Construct a fully specified MetricMaker.
     *
     * @param metrics  A mapping of metric names to the corresponding LogicalMetrics. Used to resolve metric names
     * when making the logical metric.
     * @param protocolSupport  A protocol support to use on default metric maker builds
     */
    public BaseProtocolMetricMaker(MetricDictionary metrics, ProtocolSupport protocolSupport) {
        super(metrics);
        this.baseProtocolSupport = protocolSupport;
    }

    @Override
    protected LogicalMetric makeInner(LogicalMetricInfo logicalMetricInfo, List<String> dependentMetrics) {
        return makeInnerWithResolvedDependencies(logicalMetricInfo, resolveDependencies(metrics, dependentMetrics));
    }

    @Override
    public LogicalMetric makeInnerWithResolvedDependencies(
            LogicalMetricInfo logicalMetricInfo,
            List<LogicalMetric> dependentMetrics
    ) {
        TemplateDruidQuery partialQuery = makePartialQuery(logicalMetricInfo, dependentMetrics);
        ResultSetMapper calculation = makeCalculation(logicalMetricInfo, dependentMetrics);
        ProtocolSupport protocolSupport = makeProtocolSupport(logicalMetricInfo, dependentMetrics);
        return new ProtocolMetricImpl(logicalMetricInfo, partialQuery, calculation, protocolSupport);
    }

    /**
     * Create the post processing mapper for this LogicalMetric.
     *
     * @param logicalMetricInfo  The identity metadata for the metric
     * @param dependentMetrics  The metrics this metric depends on
     *
     * @return  A mapping function to apply to the result set containing this metric
     */
    protected ResultSetMapper makeCalculation(
            final LogicalMetricInfo logicalMetricInfo, final List<LogicalMetric> dependentMetrics
    ) {
        return NO_OP_MAPPER;
    }
    /**
     * Create the partial query for this LogicalMetric.
     *
     * @param logicalMetricInfo  The identity metadata for the metric
     * @param dependentMetrics  The metrics this metric depends on
     *
     * @return  A model describing the query formula for this metric
     */
    abstract protected TemplateDruidQuery makePartialQuery(
            LogicalMetricInfo logicalMetricInfo,
            List<LogicalMetric> dependentMetrics
    );

    /**
     * Create the protocol support for this LogicalMetric.
     *
     * @param logicalMetricInfo  The identity metadata for the metric
     * @param dependentMetrics  The metrics this metric depends on
     *
     * @return  A protocol support instance defining and implementing protocols for Metrics from this maker
     */
    protected ProtocolSupport makeProtocolSupport(
            LogicalMetricInfo logicalMetricInfo,
            List<LogicalMetric> dependentMetrics
    ) {
        return baseProtocolSupport;
    }
}
