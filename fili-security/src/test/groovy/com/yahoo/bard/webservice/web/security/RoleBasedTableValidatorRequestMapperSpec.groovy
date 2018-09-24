// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext
import java.util.function.Predicate

class RoleBasedTableValidatorRequestMapperSpec extends Specification {

    RequestMapper<DataApiRequest> next = Mock(RequestMapper)

    ResourceDictionaries dictionaries = Mock(ResourceDictionaries)

    SecurityContext securityContext1 = Mock(SecurityContext)
    SecurityContext securityContext2 = Mock(SecurityContext)

    @Shared
    Map<String, Predicate<SecurityContext>> securityRules = new HashMap<>()

    Predicate<SecurityContext> predicate

    @Shared
    DataApiRequest request1
    @Shared
    DataApiRequest request2
    @Shared
    ContainerRequestContext containerRequestContext1 = Mock(ContainerRequestContext)
    @Shared
    ContainerRequestContext containerRequestContext2 = Mock(ContainerRequestContext)

    RoleBasedTableValidatorRequestMapper mapper

    def setup() {
        LogicalTable table1 = Mock(LogicalTable) {
            getName() >> "TABLE1"
        }

        request1 = Mock(DataApiRequest) {
            getTable() >> table1
        }

        LogicalTable table2 = Mock(LogicalTable) {
            getName() >> "TABLE2"
        }

        request2 = Mock(DataApiRequest) {
            getTable() >> table2
        }

        predicate = {securityContext.isUserInRole("GRANT_TABLE1")} as Predicate<SecurityContext>
        securityRules.put("TABLE1", predicate)

        predicate = {securityContext.isUserInRole("GRANT_TABLE2")} as Predicate<SecurityContext>
        securityRules.put("TABLE2", predicate)

        securityContext1.isUserInRole("GRANT_TABLE1") >> true
        containerRequestContext1.setSecurityContext(securityContext1)

        securityContext2.isUserInRole("GRANT_TABLE2") >> true
        containerRequestContext2.setSecurityContext(securityContext2)
    }

    @Unroll
    def "check if tables matching user role are alone queryable"() {
        setup:
        mapper = new RoleBasedTableValidatorRequestMapper(
                securityRules,
                dictionaries,
                next
        )

        expect:
        mapper.internalApply(request, context) == response

        where:
        request       | context                    | response
        request1      | containerRequestContext1   | request1
        request2      | containerRequestContext2   | request2
    }

}
