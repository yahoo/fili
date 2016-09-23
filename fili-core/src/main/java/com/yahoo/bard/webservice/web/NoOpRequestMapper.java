// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Default RequestMapper implementation.
 *
 * @param <T> Type of ApiRequest that this mapper maps
 */
public class NoOpRequestMapper<T extends ApiRequest> extends RequestMapper<T> {
    /**
     * Constructor.
     *
     * @param resourceDictionaries  Dictionaries to use when doing the mapping.
     */
    public NoOpRequestMapper(@NotNull ResourceDictionaries resourceDictionaries) {
        super(resourceDictionaries);
    }

    @Override
    public T apply(T request, ContainerRequestContext context) throws RequestValidationException {
        return request;
    }
}
