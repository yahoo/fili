// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.metric.LogicalMetric;

import java.util.Set;

/**
 * Metrics API Request. Such an API Request binds, validates, and models the parts of a request to the tables endpoint.
 */
public interface MetricsApiRequest extends ApiRequest {
    String REQUEST_MAPPER_NAMESPACE = "metricsApiRequestMapper";

    /**
     * Returns a set of all available metrics.
     *
     * @return the set of all available metrics
     */
    Set<LogicalMetric> getMetrics();

    /**
     * Returns an available metric.
     *
     * @return an available metric
     */
    LogicalMetric getMetric();
}
