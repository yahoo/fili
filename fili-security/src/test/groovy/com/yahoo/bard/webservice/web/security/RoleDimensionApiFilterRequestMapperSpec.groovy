// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import static com.yahoo.bard.webservice.web.FilterOperation.notin

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.DataApiRequest
import com.yahoo.bard.webservice.web.FilterOperation
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.RequestValidationException

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.attribute.UserPrincipal
import java.util.stream.Stream

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext

class RoleDimensionApiFilterRequestMapperSpec extends Specification {

    @Shared Dimension filterDimension = Mock(Dimension)
    @Shared Dimension nonFilterDimension = Mock(Dimension)
    @Shared DimensionField dimensionField = Mock(DimensionField)
    @Shared FilterOperation operation = notin

    @Shared ApiFilter mergedAFilter = new ApiFilter(filterDimension, dimensionField, operation, (["1", "2", "a"] as Set))
    @Shared ApiFilter mergedABFilter = new ApiFilter(filterDimension, dimensionField, operation, (["1", "2", "3", "a"] as Set))

    ApiFilter security1 = Mock(ApiFilter)
    ApiFilter security2 = Mock(ApiFilter)
    ApiFilter security3 = Mock(ApiFilter)

    ApiFilter requestFilterInDimension = Mock(ApiFilter)
    ApiFilter requestFilterNotInDimension = Mock(ApiFilter)



    Map<Dimension, Set<ApiFilter>> requestFilters = [(filterDimension)   : ([requestFilterInDimension] as Set),
                                                     (nonFilterDimension): ([requestFilterNotInDimension] as Set)]

    Set<ApiFilter> securitySetRoleA = [security1, security2] as Set
    Set<ApiFilter> securitySetRoleB = [security3] as Set

    Map<String, Set<ApiFilter>> roles = [A: securitySetRoleA, B: securitySetRoleB]
    RequestMapper<DataApiRequest> next = Mock(RequestMapper)

    ResourceDictionaries dictionaries = Mock(ResourceDictionaries)


    RoleDimensionApiFilterRequestMapper mapper = new RoleDimensionApiFilterRequestMapper(
            dictionaries,
            filterDimension,
            roles,
            next
    )

    UserPrincipal userPrincipal = Mock(UserPrincipal)
    SecurityContext securityContext = Mock(SecurityContext)
    ContainerRequestContext context = Mock(ContainerRequestContext)

    def setup() {
        context.getSecurityContext() >> securityContext
        [security1, security2, security3, requestFilterInDimension].each {
            it.getDimension() >> filterDimension
            it.getDimensionField() >> dimensionField
            it.operation >> operation
        }

        security1.getValues() >> (["1"] as Set)
        security2.getValues() >> (["2"] as Set)
        security3.getValues() >> (["3"] as Set)
        requestFilterInDimension.getValues() >> (["a"] as Set)
    }

    @Unroll
    def "Build security filters with roles #roleNames merges filters"() {
        setup:
        roleNames.each {
            securityContext.isUserInRole(it) >> true
        }

        expect:
        mapper.buildSecurityFilters(securityContext) == ([built] as Set)

        where:
        roleNames                | built
        (["A"] as Set)           | new ApiFilter(filterDimension, dimensionField, operation, (["1", "2"] as Set))
        (["B"] as Set)           | new ApiFilter(filterDimension, dimensionField, operation, (["3"] as Set))
        (["A", "B"] as Set)      | new ApiFilter(filterDimension, dimensionField, operation, (["1", "2", "3"] as Set))
        (["A", "B", "C"] as Set) | new ApiFilter(filterDimension, dimensionField, operation, (["1", "2", "3"] as Set))
    }

    def "Test mergeSecurityFilters merges on matching dimension and not other dimensions"() {
        setup:
        Map<Dimension, Set<ApiFilter>> expected = [(filterDimension)   : ([mergedAFilter] as Set),
                                                   (nonFilterDimension): ([requestFilterNotInDimension] as Set)]

        expect:
        mapper.mergeSecurityFilters(requestFilters, securitySetRoleA) == expected
    }

    def "validateSecurityFilters throws exception on empty security filters"() {
        setup:
        filterDimension.getApiName() >> "TestApiName"

        when:
        mapper.validateSecurityFilters(userPrincipal, [] as Set)

        then:
        thrown(RequestValidationException)
        1 * userPrincipal.getName() >> "TestUser"
    }

    @Unroll
    def "Filters #filterOne and #filterTwo  merge to #mergedFilters"() {
        setup:
        ApiFilter one = new ApiFilter(*filterOne)
        ApiFilter two = new ApiFilter(*filterTwo)
        Set<ApiFilter> expected = mergedFilters.collect {
            new ApiFilter(*it)
        }

        expect:
        RoleDimensionApiFilterRequestMapper.unionMergeFilterValues(Stream.of(one, two)) == expected

        where:
        filterOne | filterTwo | mergedFilters
        [filterDimension, dimensionField, notin, ["a", "b"] as Set] | [filterDimension, dimensionField, notin, ["a", "c"] as Set] |
                [[filterDimension, dimensionField, notin, ["a", "b", "c"] as Set]]
        [filterDimension, dimensionField, notin, ["a", "b"] as Set] | [nonFilterDimension, dimensionField, notin, ["a", "c"]  as Set] |
                [
                        [filterDimension, dimensionField, notin, ["a", "b"] as Set],
                        [nonFilterDimension, dimensionField, notin, ["a", "c"] as Set]
                ]
    }
}
