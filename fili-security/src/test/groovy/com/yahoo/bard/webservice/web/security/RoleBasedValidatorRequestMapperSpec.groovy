// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.bard.webservice.web.NoOpRequestMapper;
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest
import com.yahoo.bard.webservice.web.apirequest.utils.TestingDataApiRequestImpl
import com.yahoo.bard.webservice.web.filters.ApiFilters;

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.Collections;

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext
import java.util.function.Predicate

class RoleBasedValidatorRequestMapperSpec extends Specification {

    @Shared
    ResourceDictionaries dictionaries = new ResourceDictionaries()

    @Shared
    RequestMapper<DataApiRequest> next = new NoOpRequestMapper(dictionaries)

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
    RoleBasedValidatorRequestMapper mapper

    @Shared
    RoleBasedValidatorRequestMapper reverseNameMapper

    def setupSpec() {
        TableGroup emptyTableGroup = new TableGroup(
                new LinkedHashSet<>(),
                Collections.emptySet(),
                Collections.emptySet(),
                new ApiFilters()
        )
        LogicalTable table1 = new LogicalTable(
                "TABLE1",
                DAY,
                emptyTableGroup,
                dictionaries.getMetricDictionary()
        )

        request1 = TestingDataApiRequestImpl.buildStableDataApiRequestImpl().withTable(table1)

        LogicalTable table2 = new LogicalTable("TABLE2", DAY, emptyTableGroup, dictionaries.getMetricDictionary())

        request2 = TestingDataApiRequestImpl.buildStableDataApiRequestImpl().withTable(table2)

        predicate = {it.isUserInRole("GRANT_TABLE1")} as Predicate<SecurityContext>
        securityRules.put("TABLE1", predicate)

        predicate = {it.isUserInRole("GRANT_TABLE2")} as Predicate<SecurityContext>
        securityRules.put("TABLE2", predicate)

        predicate = {it.isUserInRole("GRANT_TABLE2")} as Predicate<SecurityContext>
        securityRules.put("2ELBAT", predicate)

        predicate = { r -> false}
        securityRules.put(RoleBasedValidatorRequestMapper.DEFAULT_SECURITY_MAPPER_NAME, predicate);

        securityContext1.isUserInRole("GRANT_TABLE1") >> true
        containerRequestContext1.getSecurityContext() >> securityContext1

        securityContext2.isUserInRole("GRANT_TABLE2") >> true
        containerRequestContext2.getSecurityContext() >> securityContext2

        mapper = new RoleBasedValidatorRequestMapper(
                securityRules,
                dictionaries,
                next,
                r -> r.getTable().getName()
        )

        reverseNameMapper = new RoleBasedValidatorRequestMapper(
                securityRules,
                dictionaries,
                next,
                (r) -> r.getTable().getName().reverse()
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
    def "check if tables whose name in reverse matches user role are queryable"() {
        expect:
        reverseNameMapper.internalApply(request, context) == response

        where:
        request       | context                    | response
        request2      | containerRequestContext2   | request2
    }

    @Unroll
    def "check if tables whose name is not in reverse matches user role are queryable"() {
        when:
        reverseNameMapper.internalApply(request, context)

        then:
        thrown exception

        where:
        request       | context                    | exception
        request1      | containerRequestContext1   | RequestValidationException

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
