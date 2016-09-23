// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import javax.ws.rs.core.Response;

/**
 * Additional HTTP Status Codes not defined in javax.ws.rs.core.Response.Status.
 */
public enum ResponseCode implements Response.StatusType {
    RATE_LIMIT(429, "Rate Limit Reached"),
    INSUFFICIENT_STORAGE(507, "Insufficient Storage");

    private final int code;
    private final String reason;
    private final Response.Status.Family family;

    /**
     * Constructor.
     *
     * @param code  Status code
     * @param reason  Reason for the status code
     */
    ResponseCode(int code, String reason) {
        this.code = code;
        this.reason = reason;
        this.family = Response.Status.Family.familyOf(code);
    }

    public int getStatusCode() {
        return code;
    }

    public Response.Status.Family getFamily() {
        return family;
    }

    public String getReasonPhrase() {
        return reason;
    }
}
