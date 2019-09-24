// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Exception used when constructing an object for a {@link DataApiRequest} but the dependencies for this object have not
 * yet been created. This is likely due to a programming error where dependency graph for the pieces of a
 * DataApiRequest has been violated. This dependency graph can be found at TODO link the graph.
 *
 */
public class UnsatisfiedApiRequestConstraintsException extends RuntimeException {

    private final String resource;
    private final Collection<String> dependencies;

    public static final String UNSATISFIED_CONSTRAINTS_MESSAGE = "%s depends on %s, but %s " +
            "has not been generated yet. Ensure the %s generation stage always runs before the " +
            "%s generation stage";

    /**
     * Constructor.
     *
     * @param resource  The name of the resource that was attempted to be built without its dependencies
     * @param dependencies  The names of the dependencies of the resource that were violated. Dependencies that were met
     *        should not be included in this collection
     */
    public UnsatisfiedApiRequestConstraintsException(String resource, Collection<String> dependencies) {
        super(generateExceptionMessage(resource, dependencies));
        this.resource = resource;
        this.dependencies = Collections.unmodifiableCollection(new ArrayList<>(dependencies));
    }

    /**
     * Constructor.
     *
     * @param resource  The name of the resource that was attempted to be built without its dependencies
     * @param dependencies  The names of the dependencies of the resource that were violated. Dependencies that were met
     *        should not be included in this collection
     * @param throwable  The source of the error to be propagated up
     */
    public UnsatisfiedApiRequestConstraintsException(
            String resource,
            Collection<String> dependencies,
            Throwable throwable
    ) {
        super(generateExceptionMessage(resource, dependencies), throwable);
        this.resource = resource;
        this.dependencies = Collections.unmodifiableCollection(new ArrayList<>(dependencies));
    }

    /**
     * Generates the exception message.
     *
     * @param resource  The resource that was attempted to be built without its dependencies
     * @param dependencies  The dependencies of the resource that were violated. Dependencies that were met should not
     *        be included in this collection
     * @return the generated exception message
     */
    private static String generateExceptionMessage(String resource, Collection<String> dependencies) {
        String depStr = String.join(", ", dependencies);
        return String.format(
                UNSATISFIED_CONSTRAINTS_MESSAGE,
                resource,
                depStr,
                depStr,
                depStr,
                resource
        );
    }

    /**
     * Getter.
     *
     * @return the resource name
     */
    public String getResource() {
        return resource;
    }

    /**
     * Getter.
     *
     * @return names of the dependencies of the resource.
     */
    public Collection<String> getDependencies() {
        return dependencies;
    }
}
