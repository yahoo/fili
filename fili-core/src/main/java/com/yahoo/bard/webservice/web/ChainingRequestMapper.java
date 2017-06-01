// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;

public abstract class ChainingRequestMapper<T extends ApiRequest> extends RequestMapper<T> {

    private final RequestMapper<T> next;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public ChainingRequestMapper(@NotNull ResourceDictionaries resourceDictionaries) {
        this(resourceDictionaries, null);
    }

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public ChainingRequestMapper(
            @NotNull ResourceDictionaries resourceDictionaries,
            RequestMapper<T> next
    ) {
        super(resourceDictionaries);
        this.next = next;
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
    public final T apply(T request, ContainerRequestContext context) throws RequestValidationException {
        return next == null ?
                internalApply(request, context)
                : next.apply(internalApply(request, context), context);
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
    public abstract T internalApply(T request, ContainerRequestContext context) throws RequestValidationException;
}
