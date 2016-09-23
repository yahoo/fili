// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Unchecked exception in case request to the jobs endpoint fails.
 */
public class JobRequestFailedException extends RuntimeException {

    /**
     * Build a JobRequestFailedException with the given message.
     *
     * @param message  The message explaining the exception
     */
    public JobRequestFailedException(String message) {
        super(message);
    }

    /**
     * Build a JobRequestFailedException with the given cause of the exception.
     *
     * @param cause  The cause of the exception
     */
    public JobRequestFailedException(Throwable cause) {
        super(cause);
    }

    /**
     * Build a JobNotFoundException with the specified cause and message.
     *
     * @param message  The message explaining the exception
     * @param cause  The cause of the exception
     */
    public JobRequestFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
