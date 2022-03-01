// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Implement a metric that accepts protocols.
 */
public class ProtocolMetricImpl extends LogicalMetricImpl implements ProtocolMetric {

    protected final ProtocolSupport protocolSupport;

    protected List<LogicalMetric> dependentMetrics;

    /**
     * Constructor.
     *
     * @param logicalMetricInfo  The metadata for the metric
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     *
     * @deprecated User {@link #ProtocolMetricImpl(LogicalMetricInfo, TemplateDruidQuery, ResultSetMapper, List)}
     */
    @Deprecated
    public ProtocolMetricImpl(
            @NotNull LogicalMetricInfo logicalMetricInfo,
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation
    ) {
        this(
                logicalMetricInfo,
                templateDruidQuery,
                calculation,
                DefaultSystemMetricProtocols.getStandardProtocolSupport(),
                Collections.emptyList()
        );
    }

    /**
     * Constructor.
     *
     * @param logicalMetricInfo  The metadata for the metric
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param protocolSupport  A identify and return protocols supported for this metric.
     *
     * @deprecated Use
     * {@link #ProtocolMetricImpl(LogicalMetricInfo, TemplateDruidQuery, ResultSetMapper, ProtocolSupport, List)}
     */
    @Deprecated
    public ProtocolMetricImpl(
            @NotNull LogicalMetricInfo logicalMetricInfo,
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            ProtocolSupport protocolSupport
    ) {
        this(logicalMetricInfo, templateDruidQuery, calculation, protocolSupport, Collections.emptyList());
    }

    /**
     * Constructor.
     *
     * @param logicalMetricInfo  The metadata for the metric
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param dependentMetrics Metrics from which this metric depend
     */
    public ProtocolMetricImpl(
            @NotNull LogicalMetricInfo logicalMetricInfo,
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            List<LogicalMetric> dependentMetrics
    ) {
        this(
                logicalMetricInfo,
                templateDruidQuery,
                calculation,
                DefaultSystemMetricProtocols.getStandardProtocolSupport(),
                dependentMetrics
        );
    }

    /**
     * Constructor.
     *
     * @param logicalMetricInfo  The metadata for the metric
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param protocolSupport  A identify and return protocols supported for this metric.
     * @param dependentMetrics Metrics which were used to create this metric.
     */
    public ProtocolMetricImpl(
            @NotNull LogicalMetricInfo logicalMetricInfo,
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            ProtocolSupport protocolSupport,
            List<LogicalMetric> dependentMetrics
    ) {
        super(logicalMetricInfo, templateDruidQuery, calculation);
        this.protocolSupport = protocolSupport;
        this.dependentMetrics = dependentMetrics;
    }


    @Override
    public boolean accepts(String protocolName) {
        return protocolSupport.accepts(protocolName);
    }

    @Override
    public LogicalMetric accept(GeneratedMetricInfo resultMetadata, String protocolName, Map<String, String> parameters)
            throws UnknownProtocolValueException {
        Protocol protocol = protocolSupport.getProtocol(protocolName);
        return protocol.getMetricTransformer().apply(resultMetadata, this, protocol, parameters);
    }

    @Override
    public ProtocolSupport getProtocolSupport() {
        return protocolSupport;
    }

    @Override
    public List<LogicalMetric> getDependentMetrics() {
        return dependentMetrics;
    }
    /**
     * All subclasses of {@code ProtocolMetricImpl} MUST override this method and return an instance of the subclassed
     * type. Inheriting this implementation on subclasses will cause the subclass typing to be lost!
     *
     * @inheritDocs
     */
    @Override
    public ProtocolMetric withLogicalMetricInfo(LogicalMetricInfo info) {
        return new ProtocolMetricImpl(
                info,
                renameTemplateDruidQuery(info.getName()),
                renameResultSetMapper(info.getName()),
                getProtocolSupport(),
                getDependentMetrics()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (!(o instanceof ProtocolMetric)) {
            return false;
        }
        if (!super.equals(o)) { return false; }
        final ProtocolMetric that = (ProtocolMetric) o;

        // Dependent metric comparison is a little too finicky, let's just make sure they have the same names
        List<String> theseNames = this.getDependentMetrics()
                .stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toList());

        List<String> thoseNames = that.getDependentMetrics()
                .stream()
                .map(LogicalMetric::getName)
                .collect(Collectors.toList());

        if (!Objects.equals(theseNames, thoseNames)) {
            return false;
        }
        return Objects.equals(getProtocolSupport(), that.getProtocolSupport());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), protocolSupport, dependentMetrics);
    }

    @Override
    public String toString() {
        return "ProtocolMetricImpl{" +
                super.toString() +
                ", protocolSupport=" + protocolSupport +
                ", dependentMetrics=" + dependentMetrics +
                '}';
    }
}
