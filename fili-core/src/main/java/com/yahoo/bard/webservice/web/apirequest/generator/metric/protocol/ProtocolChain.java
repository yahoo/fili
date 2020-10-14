// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo;
import com.yahoo.bard.webservice.data.metric.protocol.Protocol;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetric;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolSupport;
import com.yahoo.bard.webservice.data.metric.protocol.UnknownProtocolValueException;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr.ProtocolAntlrApiMetricParser;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Protocol Chain attempts to apply a set of protocols to a logical metric to produce additional metrics.
 *
 */
public class ProtocolChain {
    private static final Logger LOG = LoggerFactory.getLogger(ProtocolAntlrApiMetricParser.class);

    private final boolean strictValidation;

    List<Protocol> protocols;

    /**
     * Constructor.
     *
     * @param protocols  The chain of protocol instances to be attempted on ApiMetrics.
     * @param strictValidation  throw an error if a protocol's triggering parameters is present but the protocol
     * cannot be accepted.
     */
    public ProtocolChain(List<Protocol> protocols, boolean strictValidation) {
        this.protocols = protocols;
        this.strictValidation = strictValidation;
    }

    /**
     * Constructor.
     * Strict validation defaulted to off;
     *
     * @param protocols  The chain of protocol instances to be attempted on ApiMetrics.
     */
    public ProtocolChain(List<Protocol> protocols) {
        this(protocols, false);
    }

    /**
     * Dummy implementation. Result metadata is ALWAYS applied to the input metric, even if no protocol are applied!
     *
     * @param resultMetadata  The metadata for the expected metric
     * @param apiMetric  The metric described in the apiRequest
     * @param fromMetric  The base LogicalMetric to be transformed
     *
     * @return  The logical metric after all Protocols have been applied.
     * @throws UnknownProtocolValueException if the protocol data from the query cannot be applied
     */
    public LogicalMetric applyProtocols(
            GeneratedMetricInfo resultMetadata,
            ApiMetric apiMetric,
            LogicalMetric fromMetric
    ) throws UnknownProtocolValueException {

        LogicalMetric soFar = fromMetric;
        Map<String, String> parameters = new HashMap<>(apiMetric.getParameters());

        for (Protocol p: protocols) {
            if (!(soFar instanceof ProtocolMetric)) {
                // If it's not a protocol metric, it can't accept parameter signals
                // It may have been transformed in an earlier iteration
                break;
            }
            ProtocolMetric soFarProtocolMetric = (ProtocolMetric) soFar;
            String contractName = p.getContractName();
            String coreParameter = p.getCoreParameterName();

            if (! parameters.containsKey(coreParameter))  {
                // This protocol isn't being triggered
                continue;
            }

            ProtocolSupport protocolSupport = soFarProtocolMetric.getProtocolSupport();
            if (!protocolSupport.acceptsParameter(coreParameter)) {
                String message = "Protocol triggering parameter is sent on incompatible protocol.";
                if (strictValidation) {
                    throw new IllegalArgumentException(message);
                } else {
                    LOG.warn(message);
                    continue;
                }
            }
            if (! protocolSupport.accepts(contractName)) {
                // A different protocol implements this contract on this metric, so skip.
                continue;
            }
            soFar = soFarProtocolMetric.accept(resultMetadata, p.getContractName(), parameters);

            // Don't attempt to apply this core parameter on a subsequent protocol.  This avoids bootstrapping
            // issues where one protocol responds to a parameters, blacklists it on the resulting protocol and then
            // another protocol with the same triggering parameter gets evaluated.
            parameters.remove(coreParameter);
        }
        return soFar;
    }
}
