// Copyright 2022 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.servlet;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Singleton;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * Use the from: header and a service to build a user Principal based on a service provider.
 */
@Singleton
public abstract class RoleImpersonationFilter implements ContainerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RoleImpersonationFilter.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    /**
     * Property to allow translation of roles from distinct role domains into a common nomenclature.
     */
    public static final String ROLE_NAME_ALIAS_KEY = SYSTEM_CONFIG.getPackageVariableName("role_name_aliases");

    /**
     * The map of incoming roles to expected system roles.  This mapping allows reconciliation of role sources
     * with difference casing, separators, etc.
     */
    private final Map<String, String> aliases;

    /**
     * The expected structure of a From header is userName@domain.domain.
     */
    Pattern pattern = Pattern.compile("(\\w+)@([a-zA-Z.])");

    /**
     * Constructor. Builds the mapping of aliases based on system config.
     *
     */
    public RoleImpersonationFilter() {
        List<String> roleAliases = SYSTEM_CONFIG.getListProperty(ROLE_NAME_ALIAS_KEY);
        Map<String, String> inProgressAliases = new HashMap<>();

        if (!roleAliases.isEmpty()) {
            for (String aliasMapping : roleAliases) {
                String[] mapping = aliasMapping.split(":");
                if (mapping.length != 2) {
                    LOG.warn(
                            "role alias mapping retrieved from property \"role_name_aliases\" is malformed. " +
                                    "Entry that has either no value or extra values: {}",
                            aliasMapping
                    );
                    continue;
                }
                inProgressAliases.put(mapping[0], mapping[1]);
            }
        }
        aliases = Collections.unmodifiableMap(inProgressAliases);
    }

    public abstract List<String> getRolesForId(String id) throws IOException;

    /**
     * Test whether the user is authorized to impersonate using the From header.
     *
     * @param securityContext  The security context of the logged in user.
     */
    public abstract boolean isAuthorizedToImpersonate(SecurityContext securityContext) throws IOException;


    @Override
    public void filter(final ContainerRequestContext requestContext) throws IOException {

        // if okta, populate `isUserInRole`
        String fromId = requestContext.getHeaders().getFirst("From");
        if (fromId == null || fromId.equals("")) {
            return;
        }

        if (!isAuthorizedToImpersonate(requestContext.getSecurityContext())) {
            throwUnauthorizedImpersonationError(requestContext, fromId);
        }

        Matcher matcher = pattern.matcher(fromId);
        boolean matches = matcher.find();
        if (!matches) {
            throwMalformedFromHeaderError(fromId);
        }
        String userId = matcher.group(1);
        String domain = matcher.group(2);
        if (userId.isEmpty()) {
            throwMalformedFromHeaderError(fromId);
        }

        List<String> roles = null;
        try {
            roles = getRolesForId(userId);
            roles = roles.stream().map(s -> s.toLowerCase(Locale.US)).collect(Collectors.toList());
        } catch (IOException e) {
            String error = String.format("Failed authorization with from header: (%s)", fromId);
            LOG.error(error, e);
            throwImpersonatedUserAuthorizationError(fromId);
        }
        List<String> finalRoles = roles;

        SecurityContext originalSecurityContext = requestContext.getSecurityContext();
        Principal principal = originalSecurityContext.getUserPrincipal();
        DelegatedPrincipal delegatedPrincipal = new DelegatedPrincipal() {
            @Override
            public Principal deletegatedFrom() {
                return principal;
            }

            @Override
            public String getName() {
                return userId;
            }
        };
        requestContext.setSecurityContext(new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
                return delegatedPrincipal;
            }

            @Override
            public boolean isUserInRole(String role) {
                String conformedRole = aliases.getOrDefault(role, role).toLowerCase(Locale.US);
                return finalRoles.contains(conformedRole);
            }

            @Override
            public boolean isSecure() {
                return originalSecurityContext.isSecure();
            }

            @Override
            public String getAuthenticationScheme() {
                return originalSecurityContext.getAuthenticationScheme();
            }
        });
    }

    /**
     * An exception occurred where the user requesting authorization does not have the privilege to impersonate.
     *
     * @param requestContext  The request context for the current request.
     * @param fromId  The attempted impersonation.
     *
     * @throws IOException with an WebApplication FORBIDDEN status
     */
    private void throwUnauthorizedImpersonationError(
            final ContainerRequestContext requestContext,
            final String fromId
    ) {
        Response.ResponseBuilder builder = null;
        Principal principal = requestContext.getSecurityContext().getUserPrincipal();
        String response = String.format(
                "Attempted impersonation without proper authorization.  From: (%s), Attempting user: (%s)",
                fromId,
                principal.getName()
        );
        builder = Response.status(Response.Status.FORBIDDEN).entity(response);
        throw new WebApplicationException(builder.build());
    }

    /**
     * An error has occurred where the From header request doesn't match the expected format.
     *
     * @param fromId  The badly formed from header.
     *
     * @throws IOException with an WebApplication Unauthorized status
     */
    private void throwMalformedFromHeaderError(final String fromId) throws IOException {
        Response.ResponseBuilder builder = null;
        String response = String.format("From header (%s) is malformed.  Expected USERID@DOMAIN ", fromId);
        builder = Response.status(Response.Status.UNAUTHORIZED).entity(response);
        throw new WebApplicationException(builder.build());
    }

    /**
     * The id service has thrown some kind of exception when attempting to fetch impersonation credentials.
     *
     * @param fromId  The id being impersonated.
     *
     * @throws IOException with an WebApplication FORBIDDEN status
     */
    private void throwImpersonatedUserAuthorizationError(final String fromId) throws IOException {
        Response.ResponseBuilder builder = null;
        String response = String.format("From header (%s) user cannot be authorized.  Expected USERID@DOMAIN ", fromId);
        builder = Response.status(Response.Status.FORBIDDEN).entity(response);
        throw new WebApplicationException(builder.build());
    }
}
