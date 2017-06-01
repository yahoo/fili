// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security;

import static com.yahoo.bard.webservice.web.security.SecurityErrorMessageFormat.DIMENSION_MISSING_MANDATORY_ROLE;

import com.yahoo.bard.webservice.application.AbstractBinderFactory;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.ChainingRequestMapper;
import com.yahoo.bard.webservice.web.DataApiRequest;
import com.yahoo.bard.webservice.web.RequestValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class RoleDimensionApiFilterRequestMapper extends ChainingRequestMapper<DataApiRequest> {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinderFactory.class);

    public static final String SECURITY_SIGNUP_MESSAGE_KEY = "security_signup_message";
    public static final String DEFAULT_SECURITY_MESSAGE = "Your security settings do not permit access to this " +
            "resource.  Please contact your user administration for access.";

    private String signupMessage = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName(SECURITY_SIGNUP_MESSAGE_KEY),
            DEFAULT_SECURITY_MESSAGE
    ));
    public final SystemConfig systemConfig = SystemConfigProvider.getInstance();



    Dimension dimension;
    Map<String, Set<ApiFilter>> roleApiFilters;
    String defaultRoleMessage = DIMENSION_MISSING_MANDATORY_ROLE.format(dimension.getApiName());

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public RoleDimensionApiFilterRequestMapper(
            ResourceDictionaries resourceDictionaries,
            Dimension dimension,
            Map<String,  Set<ApiFilter>> roleApiFilters
    ) {
        this(resourceDictionaries, dimension, roleApiFilters, null);
    }

    /**
     * Constructor.
     *
     * @param resourceDictionaries  The dictionaries to use for request mapping.
     */
    public RoleDimensionApiFilterRequestMapper(
            final ResourceDictionaries resourceDictionaries,
            Dimension dimension,
            Map<String,  Set<ApiFilter>> roleApiFilters,
            ChainingRequestMapper<DataApiRequest> next
    ) {
        super(resourceDictionaries, next);
        this.dimension = dimension;
        this.roleApiFilters = roleApiFilters;
    }

    @Override
    public DataApiRequest internalApply(DataApiRequest request, ContainerRequestContext context)
            throws RequestValidationException {
        SecurityContext securityContext = context.getSecurityContext();

        Set<ApiFilter> securityFilters = roleApiFilters.keySet().stream()
                .filter(securityContext::isUserInRole)
                .map(roleApiFilters::get)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        if (securityFilters.size() == 0) {
            String name = securityContext.getUserPrincipal().getName();
            LOG.warn(DIMENSION_MISSING_MANDATORY_ROLE.logFormat(name, dimension.getApiName()));
            String message = DIMENSION_MISSING_MANDATORY_ROLE.format(dimension.getApiName());
            throw new RequestValidationException(Response.Status.FORBIDDEN,message, message);
        }
        Map<Dimension, Set<ApiFilter>> mergedFilters = mergeFilters(Stream.of(
                request.getFilters(),
                Collections.singletonMap(dimension, securityFilters)
        ));
        return request.withFilters(mergedFilters);
    }

    public static Map<Dimension, Set<ApiFilter>> mergeFilters(Stream<Map<Dimension, Set<ApiFilter>>> filterMaps) {
        return filterMaps
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (first, second) -> Stream.of(first, second)
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toSet())
                        )
                );
    }
}
