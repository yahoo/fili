// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Main log of a request served by the FeatureFlagServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class FeatureFlagRequest implements LogInfo {
    protected final String resource = "feature flags";
    protected final String flag;

    /**
     * Constructor.
     *
     * @param flag  The flag that was requested
     */
    public FeatureFlagRequest(String flag) {
        this.flag = flag;
    }
}
