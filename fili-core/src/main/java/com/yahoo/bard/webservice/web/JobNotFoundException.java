// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

/**
 * Class for exception thrown if a job is not found in the ApiJobStore.
 */
public class JobNotFoundException extends RuntimeException {

    /**
     * Build a JobNotFoundException with the given message.
     *
     * @param message  The message explaining the exception
     */
    public JobNotFoundException(String message) {
        super(message);
    }

    /**
     * Build a JobNotFoundException with the given cause of the exception.
     *
     * @param cause  The cause of the exception
     */
    public JobNotFoundException(Throwable cause) {
        super(cause);
    }

    /**
     * Build a JobNotFoundException with the specified cause and message.
     *
     * @param message  The message explaining the exception
     * @param cause  The cause of the exception
     */
    public JobNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
