// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

/**
 * Exception used when constructing an object for a {@link DataApiRequest} but the dependencies for this object have not
 * yet been created. This is likely due to a programming error where dependency graph for the pieces of a
 * DataApiRequest has been violated. This dependency graph can be found at TODO link the graph.
 *
 */
// TODO standardize this error message inside the exception and have exception only consume the dependency names
public class UnsatisfiedApiRequestConstraintsException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message  Describes the dependencies that are missing.
     */
    public UnsatisfiedApiRequestConstraintsException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message  Describes the dependencies that are missing.
     * @param throwable  Wraps the generating exception.
     */
    public UnsatisfiedApiRequestConstraintsException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
