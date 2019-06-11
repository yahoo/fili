// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.ChainingRequestMapper;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import java.util.Map;
import java.util.function.Predicate;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * A RequestMapper that validates table access for user based on roles that a user is associated with.
 *
 * @param <T> Type of API Request this RequestMapper will work on
 */
public class RoleBasedTableValidatorRequestMapper<T extends DataApiRequest> extends ChainingRequestMapper<T> {

    private final Map<String, Predicate<SecurityContext>> securityRules;

    /**
     * Constructor.
     * @param securityRules  A map of predicates for validating table access.
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param next  The next request mapper to process this ApiRequest
     */
    public RoleBasedTableValidatorRequestMapper(
            Map<String, Predicate<SecurityContext>> securityRules,
            ResourceDictionaries resourceDictionaries,
            @NotNull RequestMapper<T> next
    ) {
        super(resourceDictionaries, next);
        this.securityRules = securityRules;
    }


    @Override
    public T internalApply(T request, ContainerRequestContext context)
            throws RequestValidationException {
        SecurityContext securityContext = context.getSecurityContext();
        if (!securityRules.getOrDefault(request.getTable().getName(), (ignored -> true)).test(securityContext)) {
            throw new RequestValidationException(Response.Status.FORBIDDEN, "Permission Denied",
                    "Request cannot be completed as you do not have enough permission");
        }
        return request;
    }
}
