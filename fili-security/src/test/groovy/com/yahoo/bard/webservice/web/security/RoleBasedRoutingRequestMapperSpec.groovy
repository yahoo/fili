// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.web.ApiRequest
import com.yahoo.bard.webservice.web.RequestMapper

import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext

class RoleBasedRoutingRequestMapperSpec extends Specification {

    LinkedHashMap<String, RequestMapper<ApiRequest>> prioritizedRoleBasedMappers;
    RequestMapper<ApiRequest> mapperA = Mock(RequestMapper)
    RequestMapper<ApiRequest> mapperB = Mock(RequestMapper)
    RequestMapper<ApiRequest> mapperC = Mock(RequestMapper)

    ContainerRequestContext containerRequestContext = Mock(ContainerRequestContext)
    SecurityContext securityContext = Mock(SecurityContext)
    RoleBasedRoutingRequestMapper mapper
    Map<String, RequestMapper<ApiRequest>> mappers = [a: mapperA, b: mapperB, c:mapperC]

    def setup() {
        mapper = new RoleBasedRoutingRequestMapper(Mock(ResourceDictionaries), prioritizedRoleBasedMappers, mapperC)
        containerRequestContext.getSecurityContext() >> securityContext
        prioritizedRoleBasedMappers = [a: mapperA, b: mapperB]

        mapper = new RoleBasedRoutingRequestMapper(Mock(ResourceDictionaries), prioritizedRoleBasedMappers, mapperC)
    }

    def setupRoles(Set<String> roles) {
        roles.each {
            securityContext.isUserInRole(it) >> true
        }
        securityContext.isUserInRole(_) >> false
    }

    @Unroll
    def "Test delegation for roles #roles"() {
        setup:
        ApiRequest apiRequest = Mock(ApiRequest)
        setupRoles(roles)
        RequestMapper delegate = prioritizedRoleBasedMappers[nextMapper]

        [mapperA, mapperB, mapperC].each {
            int count = mappers[nextMapper] == it ? 1 : 0
            count * it.apply(apiRequest, containerRequestContext) >> apiRequest
        }

        expect:
        mapper.apply(apiRequest, containerRequestContext) == apiRequest

        where:
        nextMapper   | roles
        "a"          | ['a'] as Set
        "b"          | ['b'] as Set
        "a"          | ['a', 'b'] as Set
        "c"          | [] as Set
    }
}
