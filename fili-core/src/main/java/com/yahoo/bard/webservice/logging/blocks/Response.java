// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Corresponds mainly to the responding part of a request served by the DataServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Response implements LogInfo {
    protected final int length;
    protected final int code;
    protected final int numberOfRows;

    public Response(int length, int code, int numberOfRows) {
        this.length = length;
        this.code = code;
        this.numberOfRows = numberOfRows;
    }
}
