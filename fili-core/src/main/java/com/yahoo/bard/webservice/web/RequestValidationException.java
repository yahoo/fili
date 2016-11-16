// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import java.io.IOException;

import javax.ws.rs.core.Response;

/**
 * ApiRequest Validation Failed.
 */
public class RequestValidationException extends IOException {
    private final Response.StatusType status;
    private final String errorHttpMsg;

    /**
     * Constructor for RequestValidationException.
     *
     * @param status the HTTP Response Status to throw back to user
     * @param errorHttpMsg the error HTTP message to throw back to user
     * @param msg the error message
     */
    public RequestValidationException(Response.StatusType status, String errorHttpMsg, String msg) {
        super(msg);
        this.status = status;
        this.errorHttpMsg = errorHttpMsg;
    }

    /**
     * Constructor for RequestValidationException.
     *
     * @param status the HTTP Response Status to throw back to user
     * @param errorHttpMsg the error HTTP message to throw back to user
     * @param throwable the throwable
     */
    public RequestValidationException(Response.StatusType status, String errorHttpMsg, Throwable throwable) {
        super(throwable);
        this.status = status;
        this.errorHttpMsg = errorHttpMsg;
    }

    public Response.StatusType getStatus() {
        return status;
    }

    public String getErrorHttpMsg() {
        return errorHttpMsg;
    }
}
