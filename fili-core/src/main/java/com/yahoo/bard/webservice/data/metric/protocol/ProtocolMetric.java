// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.metric.protocol;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.Map;

/**
 * <p>Protocol metrics are metrics that have the ability to have transformations applied to them at query time. Protocol
 * metrics should generally represent valid metrics even if no calculations are applied.
 *
 * <p>This is a key feature in reducing the number of configured metric on a system while supporting a wide range of
 * calculations. By passing parameter values to a metric at the API, a chain of Protocols can by applied to a metric to
 * transform it into one of many calculated permutations.
 *
 * <p>For example, consider a case where you track the amount of unique users that visit your website in your datastore
 * under the fact column "unique_users". You represent this in your Fili instance as the metric "users". This allows you
 * to easily see how many unique users you had each day, compare the amount of users that visit the different pages on
 * your website, and more. However, just viewing unique users may not be enough. You may also want to know more
 * complicated calculations on the unique_users column, such as the average amount of daily unique users that
 * have visited your website in the past week since the queried day. This calculation can be statically configured in
 * your system as a separate metric ("pastWeekAvgUsers"), or you could leverage the ProtocolMetric API to allow the
 * "users" metric to be queryable on its own, or transformable into a daily average calculation at query time. In this
 * way, you would only have to configure 1 metric ("users") instead of 2 ("users", "pastWeekAvgUsers"), and configuring
 * "users" to support additional calculations would not increase the amount of metrics you support. Further, these
 * transformations can be enabled across any metrics for which they can apply by configuring and sharing Protocols.
 *
 * <p>Transformations against ProtocolMetrics are defined by the {@link Protocol} class. ProtocolMetrics can support
 * many different Protocols, and can be transformed by multiple Protocols at once. ProtocolMetrics must be able to
 * determine which Protocols they support, and expose this information through the result of
 * {@link ProtocolMetric#accepts}.
 *
 * <p>ProtocolMetrics must also ensure that no combination of Protocols they accept can produce an erroneous metric.
 * The {@link ProtocolMetric#accepts} method must fulfil this contract. Specifically, calls to ProtocolMetric#accepts
 * <b>must</b> return false if applying the Protocol would result in an invalid metric. The {@link ProtocolSupport}
 * class is intended to help track this information.
 *
 * <p>{@link ProtocolMetricImpl} is the default implementation of this contract, and should be a sufficient
 * implementation for most use cases.
 */
public interface ProtocolMetric extends LogicalMetric {

    /**
     * Tests whether this metric is transformable under the provided metric contract. This method must return false if
     * this metric does not support transformation by the input protocol contract, or if applying that contract would
     * result in an invalid metric.
     *
     * @param protocolName  The name of the protocol being tested.
     *
     * @return true if this metric can be transformed according to this protocol contract.
     */
    boolean accepts(String protocolName);

    /**
     * <p>Apply this protocol with these parameters to this metric and return a (typically different) metric. Because
     * the new metric has a different calculation it should probably have a different name than the base metric passed
     * in. This is especially important because the base metric could also be on the request as well as the transformed
     * metric. The output metadata should at least contain a new name and long name to annotate the result set with.
     *
     * <p>The transformed metric is not necessarily a protocol metric.
     *
     * @param resultMetadata  The metadata of the metric produced as a result of applying the calculation
     * @param protocolName  The name of the protocol to apply
     * @param parameters  A map of keys and values representing the transformation.
     *
     * @return A metric that has accepted this protocol transformation.
     *
     * @throws UnknownProtocolValueException if the values passed for the protocol is invalid for that protocol.
     */
    LogicalMetric accept(
            GeneratedMetricInfo resultMetadata,
            String protocolName,
            Map<String, String> parameters
    ) throws UnknownProtocolValueException;

    /**
     * Get the underlying protocol support for this metric.
     *
     * @return The protocol support
     */
    ProtocolSupport getProtocolSupport();
}
