// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Main log of a request served by the SlicesServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class SliceRequest implements LogInfo {
    protected final String resource = "slices";
    protected final String slice;

    /**
     * Constructor.
     *
     * @param slice  Slice being requested
     */
    public SliceRequest(String slice) {
        this.slice = slice;
    }
}
