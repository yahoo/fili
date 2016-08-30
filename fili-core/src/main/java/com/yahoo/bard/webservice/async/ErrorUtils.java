// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async;

import javax.validation.constraints.NotNull;
import javax.ws.rs.ProcessingException;

/**
 * The util method to compute and hold error message attributes based on error type.
 */
public class ErrorUtils {

    /**
     * Method to unwrap one level down if the Throwable is an instance of ProcessingException and its cause is not null.
     * Otherwise it will return the Throwable as it is.
     *
     * @param error  An error object which could be an instance of ProcessingException
     *
     * @return  One level unwrapped Throwable if it is an instance of ProcessingException. Otherwise as it is received
     */
    private static Throwable getError(Throwable error) {
        return error instanceof ProcessingException && error.getCause() != null ? error.getCause() : error;
    }

    /**
     * Method to get the reason for an error.
     *
     * @param cause  An error object to get the reason
     *
     * @return Reason for the error
     */
    public static String getReason(@NotNull Throwable cause) {
        return getError(cause).getClass().getName();
    }

    /**
     * Method to get the description for an error.
     *
     * @param cause  An error object to get the description
     *
     * @return Description for the error
     */
    public static String getDescription(@NotNull Throwable cause) {
        return String.valueOf(getError(cause).getMessage());
    }
}
