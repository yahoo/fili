// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.ApiRequest;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;

import java.util.LinkedHashMap;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

public class RoleBasedRoutingRequestMapper<T extends ApiRequest> extends RequestMapper<T> {

    LinkedHashMap<String, RequestMapper<T>> prioritizedRoleBasedMappers;
    RequestMapper<T> defaultMapper;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public RoleBasedRoutingRequestMapper(
            ResourceDictionaries resourceDictionaries,
            LinkedHashMap<String, RequestMapper<T>> prioritizedRoleBasedMappers,
            RequestMapper<T> defaultMapper
    ) {
        super(resourceDictionaries);
        this.prioritizedRoleBasedMappers = prioritizedRoleBasedMappers;
        this.defaultMapper = defaultMapper;
    }

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public RoleBasedRoutingRequestMapper(
            ResourceDictionaries resourceDictionaries,
            LinkedHashMap<String, RequestMapper<T>> prioritizedRoleBasedMappers
    ) {
        super(resourceDictionaries);
        this.prioritizedRoleBasedMappers = prioritizedRoleBasedMappers;
    }

    @Override
    public T apply(T request, ContainerRequestContext context)
            throws RequestValidationException {
        SecurityContext securityContext = context.getSecurityContext();
        RequestMapper<T> mapper = prioritizedRoleBasedMappers.keySet().stream()
                .filter(prioritizedRoleBasedMappers::containsKey)
                .map(prioritizedRoleBasedMappers::get)
                .findFirst().orElse(defaultMapper);
        return (mapper == null) ? request : mapper.apply(request, context);
    }
}
