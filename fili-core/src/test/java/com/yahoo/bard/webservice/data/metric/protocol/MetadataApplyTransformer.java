package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;

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
        return new ProtocolMetricImpl(
                resultMetadata,
                logicalMetric.getTemplateDruidQuery(),
                logicalMetric.getCalculation(),
                newSupport
        );
    }
}
