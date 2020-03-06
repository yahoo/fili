// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.Map;

/**
 * An interface for transforming metrics into other metrics.
 */
@FunctionalInterface
public interface MetricTransformer {

    /**
     * Transform a metric using a Protocol, and signal data.
     *
     * @param logicalMetric  The metric to transform.
     * @param protocol The protocol being transformed
     * @param parameterValues  The data associated with that signal.
     *
     * @return A new metric based on the signal;
     * @throws UnknownProtocolValueException if this transformer doesn't know how to accept this signal
     */
    LogicalMetric apply(LogicalMetric logicalMetric, Protocol protocol, Map<String, String> parameterValues)
            throws UnknownProtocolValueException;

    /**
     * Implemented here to illustrate test flows for errors.
     */
    MetricTransformer ERROR_THROWING_TRANSFORM = (metric, protocol, map) -> {
        throw new UnknownProtocolValueException(protocol, map);
    };

    /**
     * Implemented here to illustrate non mutating flows.
     */
    MetricTransformer IDENTITY_TRANSFORM = (metric, protocol, map) -> {
        return metric;
    };
}
