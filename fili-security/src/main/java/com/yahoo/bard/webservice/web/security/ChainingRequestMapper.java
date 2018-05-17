// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * An abstract ApiRequest Mapper that allows delegation to a subsequent mapper.
 *
 * @param <T> Type of API Request this RequestMapper will work on
 */
public abstract class ChainingRequestMapper<T extends ApiRequest> extends RequestMapper<T> {

    private final RequestMapper<T> next;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public ChainingRequestMapper(@NotNull ResourceDictionaries resourceDictionaries) {
        this(resourceDictionaries, DataApiRequestMapperUtils.identityMapper(resourceDictionaries));
    }

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param next  The next request mapper to process this ApiRequest
     */
    public ChainingRequestMapper(
            @NotNull ResourceDictionaries resourceDictionaries,
            @NotNull RequestMapper<T> next
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
     * @param context  the ContainerRequestContext containing user and request information
     *
     * @return a reference to an apiRequest, either the original one or a rewritten one
     *
     * @throws RequestValidationException with the HTTP status and user-facing error msg to abort with
     * if the request is not valid
     */
    public final T apply(T request, ContainerRequestContext context) throws RequestValidationException {
        return next.apply(internalApply(request, context), context);
    }

    /**
     * Rewrite and Validate the ApiRequest(e.g. Security check, permission check).
     * <p>
     * This should throw an exception if the given/converted request is not valid.
     *
     * @param request  the apiRequest to rewrite
     * @param context  the ContainerRequestContext containing user and request information
     *
     * @return a reference to an apiRequest, either the original one or a rewritten one
     *
     * @throws RequestValidationException with the HTTP status and user-facing error msg to abort with
     * if the request is not valid
     */
    protected abstract T internalApply(T request, ContainerRequestContext context) throws RequestValidationException;
}
