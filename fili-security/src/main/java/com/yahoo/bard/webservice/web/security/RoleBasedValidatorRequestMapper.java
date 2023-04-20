// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.ChainingRequestMapper;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * A RequestMapper that validates resource access for user based on roles that a user is associated with.
 *
 * @param <T> Type of API Request this RequestMapper will work on
 */
public class RoleBasedValidatorRequestMapper<T extends ApiRequest> extends ChainingRequestMapper<T> {

    public static String DEFAULT_SECURITY_MAPPER_NAME = "__default";

    public static Predicate<SecurityContext> NO_OP_PREDICATE =  (ignored -> true);

    private final Map<String, Predicate<SecurityContext>> securityRules;

    private final Function<T, String> securityIdSelector;

    /**
     * Constructor.
     * @param securityRules  A map of predicates for validating table access.
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param securityIdSelector A function for selecting a security group name from the context.
     * @param next  The next request mapper to process this ApiRequest
     */
    public RoleBasedValidatorRequestMapper(
            Map<String, Predicate<SecurityContext>> securityRules,
            ResourceDictionaries resourceDictionaries,
            @NotNull RequestMapper<T> next,
            Function<T, String> securityIdSelector
    ) {
        super(resourceDictionaries, next);
        this.securityRules = securityRules;
        this.securityIdSelector = securityIdSelector;
    }

    @Override
    public T internalApply(T request, ContainerRequestContext context)
            throws RequestValidationException {
        if (!validate(securityIdSelector.apply(request), context.getSecurityContext())) {
            throw new RequestValidationException(Response.Status.FORBIDDEN, "Permission Denied",
                    "Request cannot be completed as you do not have enough permission");
        }
        return request;
    }

    protected boolean validate(String securityTag, SecurityContext context) {
        Predicate<SecurityContext> isAllowed = securityRules.containsKey(securityTag) ?
                securityRules.get(securityTag) :
                securityRules.getOrDefault(DEFAULT_SECURITY_MAPPER_NAME, NO_OP_PREDICATE);
        return isAllowed.test(context);

    }
}
