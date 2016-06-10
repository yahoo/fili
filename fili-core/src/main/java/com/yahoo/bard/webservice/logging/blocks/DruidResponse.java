// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Corresponds mainly to the responding part of a request served by the DataServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DruidResponse implements LogInfo {
    protected final String druidQueryId;

    public DruidResponse(String druidQueryId) {
        this.druidQueryId = druidQueryId;
    }
}
