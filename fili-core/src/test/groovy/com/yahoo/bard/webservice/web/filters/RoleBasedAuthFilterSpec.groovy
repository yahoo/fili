// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.config.SystemConfig
import com.yahoo.bard.webservice.config.SystemConfigException
import com.yahoo.bard.webservice.config.SystemConfigProvider

import com.fasterxml.jackson.databind.ObjectMapper

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext

// Fail test if hangs
@Timeout(30)
class RoleBasedAuthFilterSpec extends Specification {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance()
    private static final String USER_ROLES = SYSTEM_CONFIG.getPackageVariableName("user_roles")

    // Memorize pre-test setting value
    String oldUserRoles

    ObjectMapper objectMapper
    ContainerRequestContext containerRequestContext
    SecurityContext securityContext
    RoleBasedAuthFilter roleBasedAuthFilter

    def setup() {
        objectMapper = Mock(ObjectMapper)
        containerRequestContext = Mock(ContainerRequestContext)
        securityContext = Mock(SecurityContext)
        containerRequestContext.getSecurityContext() >> securityContext
        roleBasedAuthFilter = new RoleBasedAuthFilter(objectMapper)

        try {
            oldUserRoles = SYSTEM_CONFIG.getStringProperty(USER_ROLES)
        } catch(SystemConfigException ignored) {
            oldUserRoles = null
        }
        SYSTEM_CONFIG.setProperty(USER_ROLES, "foo,bar");
    }

    def cleanup() {
        // Restore pre-test setting value if exists, clear otherwise
        SYSTEM_CONFIG.resetProperty(USER_ROLES, oldUserRoles)
    }

    def "GET request is not authorized if the user is not in an allowed role"() {
        given: "received a request and that the user is not in the authorized roles"
        containerRequestContext.getMethod() >> "GET"
        securityContext.isUserInRole(_) >> false

        expect: "the request is not authorized"
        !roleBasedAuthFilter.isAuthorized(containerRequestContext)
    }

    def "GET request is authorized if the user is in an allowed role"() {
        given: "received a request and that the user is in the authorized roles"
        containerRequestContext.getMethod() >> "GET"
        securityContext.isUserInRole("bar") >> true

        expect: "the request is authorized"
        roleBasedAuthFilter.isAuthorized(containerRequestContext)
    }

    def "GET request is authorized if the `user_roles` setting is not set"() {
        given: "received a request and that the user is not in the authorized roles"
        containerRequestContext.getMethod() >> "GET"
        SYSTEM_CONFIG.clearProperty(USER_ROLES)
        securityContext.isUserInRole(_) >> false

        expect: "the request is authorized"
        roleBasedAuthFilter.isAuthorized(containerRequestContext)
    }

    def "OPTIONS request is authorized which bypasses the filter"() {
        given: "received an OPTIONS request and that the user is in the authorized roles"
        securityContext.isUserInRole(_) >> false
        containerRequestContext.getMethod() >> "OPTIONS"

        expect: "the request is authorized"
        roleBasedAuthFilter.isAuthorized(containerRequestContext)
    }
}
