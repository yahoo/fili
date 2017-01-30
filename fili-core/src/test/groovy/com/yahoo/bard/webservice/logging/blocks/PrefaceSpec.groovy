// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging.blocks

import spock.lang.Specification

import java.nio.file.attribute.UserPrincipal

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.SecurityContext
import javax.ws.rs.core.UriInfo

class PrefaceSpec extends Specification {

    ContainerRequestContext mockRequest = Mock(ContainerRequestContext) {
        // Implement things we don't care about but Preface needs
        getUriInfo() >> Mock(UriInfo) {
            getRequestUri() >> "http://example.com".toURI()
        }
        getMethod() >> ""
        getHeaders() >> Mock(MultivaluedMap)
    }

    def "User set to default value if userPrincipal is null"() {
        given: "A request with no userPrincipal"
        mockRequest.getSecurityContext() >> Mock(SecurityContext) {
            getUserPrincipal() >> null
        }

        expect: "Making a Preface with it sets the user to the default value"
        new Preface(mockRequest).user == Preface.NO_USER_PRINCIPAL_USER_NAME
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
        new Preface(mockRequest).user == knownUserName
    }
}
