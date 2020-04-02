// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.protocol.GeneratedMetricInfo;
import com.yahoo.bard.webservice.data.metric.protocol.Protocol;
import com.yahoo.bard.webservice.data.metric.protocol.ProtocolMetric;
import com.yahoo.bard.webservice.data.metric.protocol.UnknownProtocolValueException;
import com.yahoo.bard.webservice.web.apirequest.generator.metric.antlr.ProtocolAntlrApiMetricParser;
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
    )
            throws UnknownProtocolValueException {
        LogicalMetric soFar = fromMetric;
        for (Protocol p: protocols) {
            if (! apiMetric.getParameters().containsKey(p.getCoreParameterName()))  {
                continue;
            }

            if (!(soFar instanceof ProtocolMetric)) {
                break;
            }

            ProtocolMetric soFarProtocol = (ProtocolMetric) soFar;
            if (!soFarProtocol.accepts(p.getContractName())) {
                String message = "Protocol triggering parameter is sent on incompatible protocol.";
                if (strictValidation) {
                    throw new IllegalArgumentException(message);
                } else {
                    LOG.warn(message);
                    continue;
                }
            }

            soFar = soFarProtocol.accept(resultMetadata, p.getContractName(), apiMetric.getParameters());
        }
        return soFar;
    }
}
