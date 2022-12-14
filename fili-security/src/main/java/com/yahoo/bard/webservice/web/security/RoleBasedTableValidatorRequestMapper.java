// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.ChainingRequestMapper;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequestWithTable;

import java.util.Map;
import java.util.function.Function;
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
public class RoleBasedTableValidatorRequestMapper<T extends ApiRequestWithTable> extends ChainingRequestMapper<T> {

    public static String DEFAULT_SECURITY_MAPPER_NAME = "__default";

    public static Predicate<SecurityContext> NO_OP_PREDICATE =  (ignored -> true);

    private final Map<String, Predicate<SecurityContext>> securityRules;

    private final Function<T, String> securityContextSelector;

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
        this(securityRules, resourceDictionaries, next, r -> r.getTable().getName());
    }

    /**
     * Constructor.
     * @param securityRules  A map of predicates for validating table access.
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param securityContextSelector A function for selecting a security group name from the context.
     * @param next  The next request mapper to process this ApiRequest
     */
    public RoleBasedTableValidatorRequestMapper(
            Map<String, Predicate<SecurityContext>> securityRules,
            ResourceDictionaries resourceDictionaries,
            @NotNull RequestMapper<T> next,
            Function<T, String> securityContextSelector
    ) {
        super(resourceDictionaries, next);
        this.securityRules = securityRules;
        this.securityContextSelector = securityContextSelector;
    }

    @Override
    public T internalApply(T request, ContainerRequestContext context)
            throws RequestValidationException {
        SecurityContext securityContext = context.getSecurityContext();
        String securityTag = securityContextSelector.apply(request);
        Predicate<SecurityContext> isAllowed = securityRules.containsKey(securityTag) ?
                securityRules.get(securityTag) :
                securityRules.getOrDefault(DEFAULT_SECURITY_MAPPER_NAME, NO_OP_PREDICATE);

        if (!isAllowed.test(securityContext)) {
            throw new RequestValidationException(Response.Status.FORBIDDEN, "Permission Denied",
                    "Request cannot be completed as you do not have enough permission");
        }
        return request;
    }
}
