// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks

import com.yahoo.bard.webservice.application.ObjectMappersSuite

import spock.lang.Specification

import java.nio.file.attribute.UserPrincipal
import java.security.Principal

import javax.security.auth.Subject
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedHashMap
import javax.ws.rs.core.SecurityContext
import javax.ws.rs.core.UriInfo

class PrefaceSpec extends Specification {
    private static final MAPPER = new ObjectMappersSuite().getMapper()

    ContainerRequestContext mockRequest = Mock(ContainerRequestContext) {
        // Implement things we don't care about but Preface needs
        getUriInfo() >> Mock(UriInfo) {
            getRequestUri() >> "http://example.com".toURI()
        }
        getMethod() >> ""
        getHeaders() >> new MultivaluedHashMap()
    }

    def "User set to default value if userPrincipal is null"() {
        given: "A request with no userPrincipal"
        mockRequest.getSecurityContext() >> Mock(SecurityContext) {
            getUserPrincipal() >> null
        }

        expect: "Making a Preface with it sets the user to the default value"
        def preface = new Preface(mockRequest)
        preface.getUser() == Preface.NO_USER_PRINCIPAL_USER_NAME
        MAPPER.valueToTree(preface).toString() == """{"uri":"http://example.com","method":"","headers":{"Cookie":["Cookies not present in header"]},"user":"NO_USER_PRINCIPAL"}"""
    }

    def "User set to username value if userPrincipal is not null"() {
        given: "A request with a userPrincipal"
        String knownUserName = "Known User Name"
        mockRequest.getSecurityContext() >> Mock(SecurityContext) {
            getUserPrincipal() >> Mock(UserPrincipal) {
                getName() >> knownUserName
            }
        }

        expect: "Making a Preface with it sets the user to the user name"
        def preface = new Preface(mockRequest)
        preface.getUser() == knownUserName
        MAPPER.valueToTree(preface).toString() == """{"uri":"http://example.com","method":"","headers":{"Cookie":["Cookies not present in header"]},"user":"${knownUserName}"}"""
    }

    def "User set to username value at serialization time"() {
        given: "A request with a userPrincipal"
        def securityContext = new MockSecurityContext(null)
        mockRequest.getSecurityContext() >> securityContext
        def preface = new Preface(mockRequest)

        expect: "Use default username value"
        preface.getUser() == Preface.NO_USER_PRINCIPAL_USER_NAME

        when: "Set the username value"
        String knownUserName = "Known User Name"
        def userPrincipal = new MockUserPrincipal(knownUserName)
        securityContext.setUserPrincipal(userPrincipal)

        then: "Use the set username value"
        preface.getUser() == knownUserName
    }

    static class MockUserPrincipal implements UserPrincipal {
        private final String name

        MockUserPrincipal(String name) {
            this.name = name
        }

        @Override
        String getName() {
            return name
        }

        @Override
        boolean implies(Subject subject) { return false }
    }

    static class MockSecurityContext implements SecurityContext {
        private UserPrincipal userPrincipal
        MockSecurityContext(UserPrincipal userPrincipal) {
            this.userPrincipal = userPrincipal;
        }

        void setUserPrincipal(UserPrincipal userPrincipal) {
            this.userPrincipal = userPrincipal;
        }

        @Override
        Principal getUserPrincipal() {
            return userPrincipal
        }

        @Override
        boolean isUserInRole(String role) { return false }
        @Override
        boolean isSecure() { return false }
        @Override
        String getAuthenticationScheme() { return null }
    }
}
