// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Main log of a request served by the DimensionsServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DimensionRequest implements LogInfo {
    protected final String resource = "dimensions";
    protected final String dimension;
    protected final String withValues;
    // TODO: Make this a boolean. A boolean-like String is sub-par

    /**
     * Constructor.
     *
     * @param dimension  Dimension being requested
     * @param withValues  Text string indicating if the dimension values were requested or not
     */
    public DimensionRequest(String dimension, String withValues) {
        this.dimension = dimension;
        this.withValues = withValues;
    }
}
