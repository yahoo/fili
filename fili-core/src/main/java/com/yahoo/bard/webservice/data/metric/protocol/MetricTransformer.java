// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.config.metric.makers.BaseProtocolMetricMaker;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.protocol.protocols.TimeAverageMetricTransformer;

import java.util.Map;

/**
 * <p>One of the core interfaces of the Protocol Metric API. Metric transformers apply a transform to an incoming metric
 * to produce a new outgoing metric. As part of the Protocol Metric API, MetricTransformers are attached to
 * {@link Protocol}s. MetricTransformers are utilized when the Protocol they are attached to is applied, and as such
 * they run at query time, during the API request building phase.
 *
 * MetricTransformers take parameters from the query and use that to apply their transforms to the target metric. These
 * parameters must first be parsed by the input device and are provided to the Transformer as a map of String to String.
 *
 * The value of the core parameter of the Protocol is intended to help the transformer determine what type of transform
 * it should apply. For example, {@link TimeAverageMetricTransformer.TimeAverageMetricMakerConfig} binds different
 * {@link BaseProtocolMetricMaker} instances to different supported values of the core parameter. Auxiliary
 * parameters can be placed into the input parameter map, but there is no supported way to bind the transformer to those
 * parameters. Namespacing, binding auxiliary parameter names, and ensuring the proper parameters are sent to
 * transformers are let to the client MetricTransformer and input device implementations.
 */
@FunctionalInterface
public interface MetricTransformer {

    /**
     * Transform a metric using a Protocol, and signal data.
     *
     * @param resultMetadata  The metadata of the resulting metric. After applying the transform to the input metric,
     *                        this metadata should be used. resultMetadata should NOT be shared with the input logical
     *                        metric, unless there is a case specific reason to do so.
     * @param logicalMetric  The metric to transform.
     * @param protocol The protocol guiding the transformation.
     * @param parameterValues  The data associated with that signal.
     *
     * @return A new metric based on the signal;
     * @throws UnknownProtocolValueException if this transformer doesn't know how to accept this signal
     */
    LogicalMetric apply(
            GeneratedMetricInfo resultMetadata,
            LogicalMetric logicalMetric,
            Protocol protocol,
            Map<String, String> parameterValues
    ) throws UnknownProtocolValueException;

    /**
     * Implemented here to illustrate test flows for errors.
     */
    MetricTransformer ERROR_THROWING_TRANSFORM = (lmi, metric, protocol, map) -> {
        throw new UnknownProtocolValueException(protocol, map);
    };

    /**
     * Implemented here to illustrate non mutating flows.
     */
    MetricTransformer IDENTITY_TRANSFORM = (lmi, metric, protocol, map) -> {
        return metric;
    };
}
