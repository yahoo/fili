// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;

import java.util.Collections;
import java.util.Map;

/**
 * Simple test transformer that creates a new logical metric based on the input metric. Uses the result metadata and
 * blacklists the provided protocol, but otherwise leaves the metric unchanged.
 */
public class MetadataApplyTransformer implements MetricTransformer {

    @Override
    public LogicalMetric apply(
            GeneratedMetricInfo resultMetadata,
            LogicalMetric logicalMetric,
            Protocol protocol,
            Map<String, String> parameterValues
    ) throws UnknownProtocolValueException {
        ProtocolSupport oldSupport = (logicalMetric instanceof ProtocolMetric) ?
                ((ProtocolMetric) logicalMetric).getProtocolSupport() :
                new ProtocolSupport(Collections.emptySet());
        ProtocolSupport newSupport = oldSupport.blacklistProtocol(protocol.getContractName());
        TemplateDruidQuery renamedTdq = logicalMetric.getTemplateDruidQuery().renameMetricField(
                logicalMetric.getLogicalMetricInfo().getName(),
                resultMetadata.getName()
        );
        return new ProtocolMetricImpl(
                resultMetadata,
                renamedTdq,
                logicalMetric.getCalculation(),
                newSupport
        );
    }
}
