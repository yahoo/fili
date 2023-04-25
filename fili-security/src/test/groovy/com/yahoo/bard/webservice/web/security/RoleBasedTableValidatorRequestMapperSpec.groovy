// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.data.time.StandardGranularityParser;
import com.yahoo.bard.webservice.druid.model.builders.DruidFilterBuilder;
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableGroup
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.NoOpRequestMapper;
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequest;
import com.yahoo.bard.webservice.web.apirequest.TablesApiRequestImpl;
import com.yahoo.bard.webservice.web.apirequest.generator.having.HavingGenerator;
import com.yahoo.bard.webservice.web.filters.ApiFilters
import com.yahoo.bard.webservice.web.util.BardConfigResources

import org.joda.time.DateTimeZone

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.Collections
import java.util.LinkedHashSet

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext
import java.util.function.Predicate

class RoleBasedTableValidatorRequestMapperSpec extends Specification {

    @Shared
    ResourceDictionaries dictionaries = new ResourceDictionaries()

    @Shared
    RequestMapper<TablesApiRequest> next = new NoOpRequestMapper(dictionaries)

    @Shared
    SecurityContext securityContext1 = Mock(SecurityContext)
    @Shared
    SecurityContext securityContext2 = Mock(SecurityContext)

    @Shared
    Map<String, Predicate<SecurityContext>> securityRules = new HashMap<>()

    @Shared
    Predicate<SecurityContext> predicate
    @Shared
    Predicate<SecurityContext> predicate2

    @Shared
    TablesApiRequest request1
    @Shared
    TablesApiRequest request2
    @Shared
    ContainerRequestContext containerRequestContext1 = Mock(ContainerRequestContext)
    @Shared
    ContainerRequestContext containerRequestContext2 = Mock(ContainerRequestContext)
    @Shared
    LogicalTable table1
    @Shared
    LogicalTable table2

    @Shared
    RoleBasedTableValidatorRequestMapper mapper

    BardConfigResources getBardConfigResources() {
        return new BardConfigResources() {
            public LogicalTableDictionary getLogicalDictionary() {
                return dictionaries.getLogicalDictionary()
            }

            public ResourceDictionaries getResourceDictionaries() {
                return dictionaries
            }

            public GranularityParser getGranularityParser() {
                return new StandardGranularityParser()
            }

            public DruidFilterBuilder getFilterBuilder() {
                return null
            }

            public HavingGenerator getHavingApiGenerator() {
                return null
            }

            public DateTimeZone getSystemTimeZone() {
                return DateTimeZone.UTC
            }
        }
    }

    TablesApiRequest testingTablesApiRequest() {
         return new TablesApiRequestImpl(
                "TABLE1",
                DefaultTimeGrain.DAY.toString(),
                "json",
                "",
                "",
                getBardConfigResources()
        )
    }

    def setupSpec() {
        securityRules.put(
                RoleBasedValidatorRequestMapper.DEFAULT_SECURITY_MAPPER_NAME,
                {
                    throw new IllegalStateException("Missing security rule. Security rules: " + securityRules)
                } as Predicate<SecurityContext>
        )
        TableGroup emptyTableGroup = new TableGroup(
                new LinkedHashSet<>(),
                Collections.emptySet(),
                Collections.emptySet(),
                new ApiFilters()
        )
        table1 = new LogicalTable(
                "TABLE1",
                DefaultTimeGrain.DAY,
                emptyTableGroup,
                dictionaries.getMetricDictionary()
        )
        table2 = new LogicalTable(
            "TABLE2",
            DefaultTimeGrain.DAY,
            emptyTableGroup,
            dictionaries.getMetricDictionary()
        )
        dictionaries.getLogicalDictionary().put(new TableIdentifier(table1), table1)
        dictionaries.getLogicalDictionary().put(new TableIdentifier(table2), table2)

        request1 = testingTablesApiRequest().withTable(table1)
        request2 = testingTablesApiRequest().withTable(table2)

        predicate = {it.isUserInRole("GRANT_TABLE1")} as Predicate<SecurityContext>
        securityRules.put("TABLE1", predicate)

        securityContext1.isUserInRole("GRANT_TABLE1") >> true
        containerRequestContext1.getSecurityContext() >> securityContext1

        securityContext2.isUserInRole("GRANT_TABLE2") >> true
        containerRequestContext2.getSecurityContext() >> securityContext2
        predicate2 = {it.isUserInRole("GRANT_TABLE2")}
        securityRules.put("TABLE2", predicate2)

        mapper = new RoleBasedTableValidatorRequestMapper(
                securityRules,
                dictionaries,
                next
        )
    }

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

    @Unroll
    def "A user with permissions #permissions can see tables #tables when fullview is requested"() {
        given:
        SecurityContext securityContext = Stub(SecurityContext)
        permissions.forEach({securityContext.isUserInRole(it) >> true})
        ContainerRequestContext requestContext = Stub(ContainerRequestContext) {
            getSecurityContext() >> securityContext
        }
        and:
        TablesApiRequest request = new TablesApiRequestImpl(null, null, "json", "", "", getBardConfigResources())

        when:
        TablesApiRequest mappedRequest = mapper.apply(request, requestContext)

        then:
        mappedRequest.getTables() == tables
    where:
        permissions                         | tables
        ["GRANT_TABLE1"]                    | [table1] as Set
        ["GRANT_TABLE2"]                    | [table2] as LinkedHashSet
        ["GRANT_TABLE1", "GRANT_TABLE2"]    | [table1, table2] as LinkedHashSet
        []                                  | [] as LinkedHashSet
    }
}
