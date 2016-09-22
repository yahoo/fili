// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks;

import com.yahoo.bard.webservice.logging.LogInfo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Main log of a request served by the JobsServlet.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class JobRequest implements LogInfo {
    protected final String resource = "jobs";
    protected final String ticket;

    /**
     * Build a JobRequest with the given ticket.
     *
     * @param ticket  ticket whose job is being requested
     */
    public JobRequest(String ticket) {
        this.ticket = ticket;
    }
}
