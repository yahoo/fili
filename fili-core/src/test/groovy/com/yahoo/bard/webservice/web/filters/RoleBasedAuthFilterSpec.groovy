// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters;

import com.yahoo.bard.webservice.application.JerseyTestBinder
import com.yahoo.bard.webservice.web.endpoints.DataServlet

import spock.lang.Specification
import spock.lang.Timeout

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext

// Fail test if hangs
@Timeout(30)
class RoleBasedAuthFilterSpec extends Specification {

    JerseyTestBinder jtb

    Class<?>[] getResourceClasses() {
        [RoleBasedAuthFilterSpec.class, DataServlet.class]
    }

    def setup() {
        // Create the test web container to test the resources
        jtb = new JerseyTestBinder(getResourceClasses())
    }

    def cleanup() {
        // Release the test web container
        jtb.tearDown()
    }

    def "GET requests return false if the user is not in an allowed role"() {
        given: "A request that does not have the security context of an user in role"
        ContainerRequestContext context = Mock(ContainerRequestContext)
        SecurityContext securityContext = Mock(SecurityContext)
        securityContext.isUserInRole("foo") >> true
        context.getSecurityContext() >> securityContext
        List<String> allowedUserRoles = ["baz"]

        and: "A role based auth filter"
        RoleBasedAuthFilter roleBasedAuthFilter = new RoleBasedAuthFilter()

        expect: "The boolean stating the user is not in role"
        roleBasedAuthFilter.isUserInRole(allowedUserRoles, context) == false
    }

    def "GET requests return true if the user is in an allowed role"() {
        given: "A request that has the security context of an user in role "
        ContainerRequestContext context = Mock(ContainerRequestContext)
        SecurityContext securityContext = Mock(SecurityContext)
        securityContext.isUserInRole("foo") >> true
        context.getSecurityContext() >> securityContext
        List<String> allowedUserRoles = ["foo","bar"]

        and: "A role based auth filter"
        RoleBasedAuthFilter roleBasedAuthFilter = new RoleBasedAuthFilter()

        expect: "The boolean stating the user is in role"
        roleBasedAuthFilter.isUserInRole(allowedUserRoles, context) == true
    }
}
