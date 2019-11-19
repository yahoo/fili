// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext
import java.util.function.Predicate

class RoleBasedTableValidatorRequestMapperSpec extends Specification {

    @Shared
    RequestMapper<DataApiRequest> next = Mock(RequestMapper)

    @Shared
    ResourceDictionaries dictionaries = Mock(ResourceDictionaries)

    @Shared
    SecurityContext securityContext1 = Mock(SecurityContext)
    @Shared
    SecurityContext securityContext2 = Mock(SecurityContext)

    @Shared
    Map<String, Predicate<SecurityContext>> securityRules = new HashMap<>()

    @Shared
    Predicate<SecurityContext> predicate

    @Shared
    DataApiRequest request1
    @Shared
    DataApiRequest request2
    @Shared
    ContainerRequestContext containerRequestContext1 = Mock(ContainerRequestContext)
    @Shared
    ContainerRequestContext containerRequestContext2 = Mock(ContainerRequestContext)

    @Shared
    RoleBasedTableValidatorRequestMapper mapper

    def setupSpec() {
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

        predicate = {it.isUserInRole("GRANT_TABLE1")} as Predicate<SecurityContext>
        securityRules.put("TABLE1", predicate)

        predicate = {it.isUserInRole("GRANT_TABLE2")} as Predicate<SecurityContext>
        securityRules.put("TABLE2", predicate)

        securityContext1.isUserInRole("GRANT_TABLE1") >> true
        containerRequestContext1.getSecurityContext() >> securityContext1

        securityContext2.isUserInRole("GRANT_TABLE2") >> true
        containerRequestContext2.getSecurityContext() >> securityContext2

        mapper = new RoleBasedTableValidatorRequestMapper(
                securityRules,
                dictionaries,
                next
        )
    }

    @Unroll
    def "check if tables matching user role are alone queryable"() {
        expect:
        mapper.internalApply(request, context) == response

        where:
        request       | context                    | response
        request1      | containerRequestContext1   | request1
        request2      | containerRequestContext2   | request2
    }

    @Unroll
    def "check if exception is thrown if user's role does not match the security rules for the logical table"() {
        when:
        mapper.internalApply(request, context)

        then:
        thrown exception

        where:
        request       | context                    | exception
        request1      | containerRequestContext2   | RequestValidationException
        request2      | containerRequestContext1   | RequestValidationException
    }
}
