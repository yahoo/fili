// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * RequestMapper will do post processing on a given ApiRequest, such as rewriting or validating queries.
 *
 * @param <T> Type of API Request this RequestMapper will work on
 */
public abstract class RequestMapper<T extends ApiRequest> {
    private final ResourceDictionaries resourceDictionaries;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public RequestMapper(@NotNull ResourceDictionaries resourceDictionaries) {
        this.resourceDictionaries = resourceDictionaries;
    }

    /**
     * Rewrite and Validate the ApiRequest(e.g. Security check, permission check).
     * <p>
     * This should throw an exception if the given/converted request is not valid.
     *
     * @param request  the apiRequest to rewrite
     * @param context  the ContainerRequestContext
     *
     * @return a reference to an apiRequest, either the original one or a rewritten one
     *
     * @throws RequestValidationException with the HTTP status and user-facing error msg to abort with
     * if the request is not valid
     */
    public abstract T apply(T request, ContainerRequestContext context) throws RequestValidationException;

    public ResourceDictionaries getResourceDictionaries() {
        return resourceDictionaries;
    }
}
