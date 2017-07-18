// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.ApiRequest;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;

import java.util.LinkedHashMap;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

/**
 * A RequestMapper that delegates to the first request mapper in a list which the user has a supporting role for.
 *
 * @param <T> Type of API Request this RequestMapper will work on
 */
public class RoleBasedRoutingRequestMapper<T extends ApiRequest> extends RequestMapper<T> {

    private final LinkedHashMap<String, RequestMapper<T>> prioritizedRoleBasedMappers;
    private final RequestMapper<T> defaultMapper;

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param prioritizedRoleBasedMappers  A map of roles to mappers for each role, with a deterministic entry order.
     * @param defaultMapper The default mapper to apply if no other roles match. (should not be null to avoid default
     * access by all)
     */
    public RoleBasedRoutingRequestMapper(
            ResourceDictionaries resourceDictionaries,
            LinkedHashMap<String, RequestMapper<T>> prioritizedRoleBasedMappers,
            @NotNull RequestMapper<T> defaultMapper
    ) {
        super(resourceDictionaries);
        this.prioritizedRoleBasedMappers = prioritizedRoleBasedMappers;
        this.defaultMapper = defaultMapper;
    }


    @Override
    public T apply(T request, ContainerRequestContext context)
            throws RequestValidationException {
        SecurityContext securityContext = context.getSecurityContext();
        return prioritizedRoleBasedMappers.keySet().stream()
                .filter(securityContext::isUserInRole)
                .map(prioritizedRoleBasedMappers::get)
                .findFirst().orElse(defaultMapper)
                .apply(request, context);
    }
}
