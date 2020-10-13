// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.metric.makers;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.MetricDictionary;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;
import com.yahoo.bard.webservice.data.metric.protocol.DefaultSystemMetricProtocols;
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetric;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetricImpl;
import com.yahoo.bard.webservice.druid.model.MetricField;

import java.util.Collections;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An expansion on the the base {@link MetricMaker} contract that leverages the functionality of {@link ProtocolMetric}.
 *
 * While extensions of the base MakerMetric class are intended to be used only at configuration time, extensions of
 * BaseProtocolMetricMaker are intended to be used both at configuration time and at query time.
 */
public abstract class BaseProtocolMetricMaker extends MetricMaker implements MakeFromMetrics {

    /**
     * The default protocol support for metrics for derived metric makers.
     */
    protected final ProtocolSupport baseProtocolSupport;

    private static final String DEFAULT_RENAMED_PREFIX = "__renamed_";

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
        LogicalMetric dependentMetric = dependentMetrics.get(0);
        LogicalMetric renamedDependentMetric = renameIfConflicting(logicalMetricInfo.getName(), dependentMetric);
        List<LogicalMetric> renamedDependentMetrics =
                dependentMetrics.size() == 1 ? Collections.singletonList(renamedDependentMetric) : dependentMetrics;
        TemplateDruidQuery partialQuery = makePartialQuery(logicalMetricInfo, renamedDependentMetrics);
        ResultSetMapper calculation = makeCalculation(logicalMetricInfo, renamedDependentMetrics);
        ProtocolSupport protocolSupport = makeProtocolSupport(logicalMetricInfo, renamedDependentMetrics);

        return new ProtocolMetricImpl(logicalMetricInfo, partialQuery, calculation, protocolSupport);
    }

    /**
     * Renames the provided logical metric if there is a name conflict between it and the final desired output name.
     * If there is no name conflict the input metric is returned unchanged. Otherwise, the input metric is renamed to
     * not conflict with the final output name.
     *
     * @param finalOutputName  The name to check for conflicts on
     * @param dependentMetric  The dependent metric to check for a conflicting name
     *
     * @return  The metric without a name conflicting with finalOutputName
     */
     protected LogicalMetric renameIfConflicting(String finalOutputName, LogicalMetric dependentMetric) {
        LogicalMetricInfo dependentMetricInfo = dependentMetric.getLogicalMetricInfo();
        //if no name conflict ,  return the original dependentMetric
        if (!java.util.Objects.equals(finalOutputName, dependentMetricInfo.getName())) {
            return dependentMetric;
        }
        String newName = getRenamedMetricNameWithPrefix(dependentMetricInfo.getName());
        while (ifConflicting(newName, dependentMetric)) {
            newName = getRenamedMetricNameWithPrefix(newName);
        }
        LogicalMetricInfo resultInfo;
        if (dependentMetricInfo instanceof GeneratedMetricInfo) {
            GeneratedMetricInfo generatedMetricInfo = (GeneratedMetricInfo) dependentMetricInfo;
            resultInfo = new GeneratedMetricInfo(newName, generatedMetricInfo.getBaseMetricName());
        } else {
            resultInfo = new LogicalMetricInfo(newName);
        }

        return dependentMetric.withLogicalMetricInfo(resultInfo);
    }

    /**
     * Method to build new renamed metric name by adding prefix to it.
     * This method would make sure all maker subclasses use unqiue prefix to build renamed name
     * to avoid naming collision.
     * @param name The dependent metric name need rename.
     *
     * @return Renamed metric name with specified prefix
     */
    protected String getRenamedMetricNameWithPrefix(String name) {
        return DEFAULT_RENAMED_PREFIX + name;
    }

    /**
     * Method to determine if there is name conflict.
     * @param newName The dependent metric new name.
     * @param dependentMetric The dependent LogicalMetric.
     *
     * @return Returns true if newName is already been used by the aggregations. otherwise false.
     */
    protected boolean ifConflicting(String newName, LogicalMetric dependentMetric) {
        Set<String> aggregations = dependentMetric.getTemplateDruidQuery().getAggregations().stream()
                .map(MetricField::getName).collect(Collectors.toSet());
        Set<String> postAggregations = dependentMetric.getTemplateDruidQuery().getPostAggregations()
                .stream().map(MetricField::getName).collect(Collectors.toSet());
        return aggregations.contains(newName) || postAggregations.contains(newName);
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
