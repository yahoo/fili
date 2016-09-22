// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigException;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * RoleBasedAuthFilter - Filter servlet calls based on the user Role.
 */
@Singleton
@Priority(4)
public class RoleBasedAuthFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RoleBasedAuthFilter.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private final ObjectMapper mapper;

    /**
     * Constructor.
     *
     * @param mapper JSON mapper to use
     */
    @Inject
    public RoleBasedAuthFilter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public void filter(ContainerRequestContext containerRequestContext) throws IOException {
        List<String> allowedUserRoles = getAllowedUserRoles();
        if (!allowedUserRoles.isEmpty() && !isUserInRole(allowedUserRoles, containerRequestContext)) {
                abortRequest(
                        containerRequestContext,
                        Response.Status.FORBIDDEN,
                        BouncerAuthorizationStatus.ACCESS_DENIED
                );
        }
    }

    /**
     * Abort the given request. Re-direct not needed as the user is already assumed to be authenticated.
     *
     * @param containerRequestContext  Request to abort
     * @param status  Status to abort the request with
     * @param reason  Reason for the abort (Usually the HTTP Status Code Reason)
     */
    private void abortRequest(
            ContainerRequestContext containerRequestContext,
            Response.Status status,
            BouncerAuthorizationStatus reason
    ) {
        LOG.debug("The user is not authorized in an authorized role ", status);

        Map<String, Object> responseMap = new LinkedHashMap<>();
        responseMap.put("status", status);
        responseMap.put("reason", reason.getName());
        responseMap.put("description", reason.getDescription());

        String json;
        try {
            json = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(responseMap);
        } catch (JsonProcessingException e) {
            json = e.getMessage();
        }
        containerRequestContext.abortWith(Response.status(status).entity(json).build());
    }

    /**
     * Gets the list of allowed user roles.
     *
     * @return List of allowed user roles
     */
    private List<String> getAllowedUserRoles() {
        try {
            return SYSTEM_CONFIG.getListProperty(
                SYSTEM_CONFIG.getPackageVariableName("user_roles")
            );
        } catch (SystemConfigException ignored) {
            return Collections.emptyList();
        }
    }

    /**
     * Checks if an user role belongs to the list of allowed roles.
     *
     * @param allowedUserRoles List of allowed roles
     * @param containerRequestContext Request context contains the user role information
     *
     * @return boolean based on the user role
     */
    public boolean isUserInRole (List<String> allowedUserRoles, ContainerRequestContext containerRequestContext) {
        SecurityContext securityContext = containerRequestContext.getSecurityContext();
        return allowedUserRoles.stream().anyMatch(securityContext::isUserInRole);
    }
}
