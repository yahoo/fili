// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.ApiRequest;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;

import java.util.function.BiFunction;

import javax.ws.rs.container.ContainerRequestContext;

public class DataApiRequestMapperUtils {

    public static RequestMapper identityMapper(ResourceDictionaries resourceDictionaries) {
        return new RequestMapper(resourceDictionaries) {
            @Override
            public ApiRequest apply(final ApiRequest request, final ContainerRequestContext context)
                    throws RequestValidationException {
                return request;
            };
        };
    };

    /**
     * Create a requestMapper that always throws a RuntimeException.
     *
     * This can be used to 'cap' a filter path such as a {@link RoleBasedRoutingRequestMapper} where the 'default'
     * should only be hit exceptionally.
     *
     * @param resourceDictionaries  The dictionaries used for mapping request.
     * @param exception  Yhr
     *
     * @return  A request mapper that will always fail on a validation exception.
     */

    public static RequestMapper errorMapper(ResourceDictionaries resourceDictionaries, RuntimeException exception) {
        return new RequestMapper(resourceDictionaries) {
            @Override
            public ApiRequest apply(final ApiRequest request, final ContainerRequestContext context)
                    throws RequestValidationException {
                throw exception;
            };
        };
    };

    /**
     * Create a requestMapper that always throws a validation exception based on the request.
     * This can be used to 'cap' a filter path such as a {@link RoleBasedRoutingRequestMapper} where the 'default'
     * should only be hit exceptionally.
     *
     * @param resourceDictionaries  The dictionaries used for mapping request.
     * @param exceptionSource  A function to create a request validation exception.
     *
     * @return  A request mapper that will always fail on a validation exception.
     */
    public static RequestMapper validationExceptionMapper(
            ResourceDictionaries resourceDictionaries,
            BiFunction<ApiRequest, ContainerRequestContext, RequestValidationException> exceptionSource
    ) {
        return new RequestMapper(resourceDictionaries) {
            @Override
            public ApiRequest apply(final ApiRequest request, final ContainerRequestContext context)
                    throws RequestValidationException {
                throw exceptionSource.apply(request, context);
            };
        };
    };

}
