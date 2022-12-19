// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.RequestMapper;
import com.yahoo.bard.webservice.web.RequestValidationException;
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

/**
 * A RequestMapper that validates table access for user based on roles that a user is associated with.
 */
public class RoleBasedTableValidatorRequestMapper extends RoleBasedValidatorRequestMapper<TablesApiRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(RoleBasedTableValidatorRequestMapper.class);

    /**
     * Constructor.
     * @param securityRules  A map of predicates for validating table access.
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     * @param next  The next request mapper to process this ApiRequest
     */
    public RoleBasedTableValidatorRequestMapper(
            Map<String, Predicate<SecurityContext>> securityRules,
            ResourceDictionaries resourceDictionaries,
            @NotNull RequestMapper<TablesApiRequest> next
    ) {
        super(securityRules, resourceDictionaries, next, r -> r.getTable().getName());
    }

    @Override
    public TablesApiRequest internalApply(TablesApiRequest request, ContainerRequestContext context)
            throws RequestValidationException {
        if (request.getTable() == null) {
            SecurityContext securityContext = context.getSecurityContext();
            LinkedHashSet<LogicalTable> exposedTables = request.getTables().stream()
                    .filter(table -> validate(table.getName(), securityContext))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            return request.withTables(exposedTables);
        }
        return super.internalApply(request, context);
    }
}
