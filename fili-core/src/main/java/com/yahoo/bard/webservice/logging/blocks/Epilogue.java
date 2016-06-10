// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import javax.ws.rs.core.Response.StatusType;

/**
 * Common information for every log block that is saved when the logging of a request is finalized.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class Epilogue implements LogInfo {
    protected final String status;
    protected final int code;
    protected final String logMessage;

    public Epilogue(String logMessage, StatusType response) {
        this.status = response.getReasonPhrase();
        this.code = response.getStatusCode();
        this.logMessage = logMessage;
    }
}
