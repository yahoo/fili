// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Main log of a request served by the MetricsServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class MetricRequest implements LogInfo {
    protected final String resource = "metrics";
    protected final String metric;

    /**
     * Constructor.
     *
     * @param metric  Metric being requested
     */
    public MetricRequest(String metric) {
        this.metric = metric;
    }
}
