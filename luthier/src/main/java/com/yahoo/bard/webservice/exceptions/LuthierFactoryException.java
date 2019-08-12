// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.exceptions;

/**
 * An exception thrown when Luthier fails to build concepts or resolve dependencies.
 */
public class LuthierFactoryException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message  Error message text
     */
    public LuthierFactoryException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message Error message text
     * @param cause  throwable triggering this exception.
     */
    public LuthierFactoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
