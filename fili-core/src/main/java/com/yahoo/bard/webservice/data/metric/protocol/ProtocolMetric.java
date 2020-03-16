// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.Map;

/**
 * Protocol metrics are capable of handling transforming signals from the client.
 *
 * This is a key feature in reducing the number of configured metric on a system while supporting a wide range of
 * calculations.  By passing parameter value to a metric at the API, a chain of Protocols can by applied to a metric
 * to transform it into one of many calculation permutations.
 */
public interface ProtocolMetric extends LogicalMetric {

    /**
     * Test whether this Metric accepts this named contract.
     *
     * @param protocolName  The name of the protocol being tested.
     *
     * @return true if this metric knows how to handle this signal type.
     */
    boolean accepts(String protocolName);

    /**
     * Apply this protocol with these parameters to this metric and return a (typically different) metric.
     *
     * The transformed metric is not necessarily a protocol metric.
     *
     * @param protocolName  The name of the protocol to apply
     * @param parameters  A map of keys and values representing the transformation.
     *
     * @return A metric that has accepted this protocol transformation.
     *
     * @throws UnknownProtocolValueException if the values passed for the protocol is invalid for that protocol.
     */
    LogicalMetric accept(String protocolName, Map<String, String> parameters) throws UnknownProtocolValueException;

    /**
     * Get the underlying protocol support for this metric.
     *
     * @return The protocol support
     */
    ProtocolSupport getProtocolSupport();
}
