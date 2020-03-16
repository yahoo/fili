// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.LogicalMetricImpl;
import com.yahoo.bard.webservice.data.metric.LogicalMetricInfo;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.data.metric.mappers.ResultSetMapper;

import java.util.Map;
import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * Implement a metric that accepts protocols.
 */
public class ProtocolMetricImpl extends LogicalMetricImpl implements ProtocolMetric {

    protected final ProtocolSupport protocolSupport;

    /**
     * Constructor.
     *
     * @param logicalMetricInfo  The metadata for the metric
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     */
    public ProtocolMetricImpl(
            @NotNull LogicalMetricInfo logicalMetricInfo,
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation
    ) {
        this(
                logicalMetricInfo,
                templateDruidQuery,
                calculation,
                DefaultSystemMetricProtocols.getStandardProtocolSupport()
        );
    }

    /**
     * Constructor.
     *
     * @param logicalMetricInfo  The metadata for the metric
     * @param templateDruidQuery  Query the metric needs
     * @param calculation  Mapper for the metric
     * @param protocolSupport  A identify and return protocols supported for this metric.
     */
    public ProtocolMetricImpl(
            @NotNull LogicalMetricInfo logicalMetricInfo,
            @NotNull TemplateDruidQuery templateDruidQuery,
            ResultSetMapper calculation,
            ProtocolSupport protocolSupport
    ) {
        super(logicalMetricInfo, templateDruidQuery, calculation);
        this.protocolSupport = protocolSupport;
    }

    @Override
    public boolean accepts(String protocolName) {
        return protocolSupport.accepts(protocolName);
    }

    @Override
    public LogicalMetric accept(String protocolName, Map<String, String> parameters)
            throws UnknownProtocolValueException {
        Protocol protocol = protocolSupport.getProtocol(protocolName);
        return protocol.getMetricTransformer().apply(this, protocol, parameters);
    }

    @Override
    public ProtocolSupport getProtocolSupport() {
        return protocolSupport;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        if (!super.equals(o)) { return false; }
        final ProtocolMetricImpl that = (ProtocolMetricImpl) o;
        return Objects.equals(protocolSupport, that.protocolSupport);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), protocolSupport);
    }

    @Override
    public String toString() {
        return "ProtocolMetricImpl{" +
                super.toString() +
                ", protocolSupport=" + protocolSupport +
                '}';
    }
}
