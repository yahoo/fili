// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.web.ChainingRequestMapper;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

/**
 * A RequestMapper that validates table access for user based on roles that a user is associated with.
 *
 */
public class RoleBasedTableValidatorRequestMapper extends ChainingRequestMapper<DataApiRequest> {

    public static String DEFAULT_SECURITY_MAPPER_NAME = "__default";

    public static final Predicate<SecurityContext> NO_OP_PREDICATE = null;

    private final Map<String, Predicate<SecurityContext>> securityRules;

    private final Function<DataApiRequest, String> securityContextSelector;

    /**
     * Constructor.
     *
     * @param securityRules  A map of predicates for validating table access.
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param next  The next request mapper to process this ApiRequest
     */
    public RoleBasedTableValidatorRequestMapper(
            Map<String, Predicate<SecurityContext>> securityRules,
            ResourceDictionaries resourceDictionaries,
            RequestMapper<DataApiRequest> next
    ) {
        this(securityRules, resourceDictionaries, next, r -> r.getTable().getName());
    }

    /**
     * Constructor.
     *
     * @param securityRules  A map of predicates for validating table access.
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param securityContextSelector A function for selecting a security group name from the context.
     * @param next  The next request mapper to process this ApiRequest
     */
    public RoleBasedTableValidatorRequestMapper(
            Map<String, Predicate<SecurityContext>> securityRules,
            ResourceDictionaries resourceDictionaries,
            RequestMapper<DataApiRequest> next,
            Function<DataApiRequest, String> securityContextSelector
    ) {
        super(resourceDictionaries, next);
        this.securityRules = securityRules;
        this.securityContextSelector = securityContextSelector;
    }

    public DataApiRequest internalApply(DataApiRequest request, ContainerRequestContext context) {
        return request;
    }
}
