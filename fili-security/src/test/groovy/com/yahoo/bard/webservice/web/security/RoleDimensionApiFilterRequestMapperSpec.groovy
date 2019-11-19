// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import static com.yahoo.bard.webservice.web.DefaultFilterOperation.notin

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.DefaultFilterOperation
import com.yahoo.bard.webservice.web.FilterOperation
import com.yahoo.bard.webservice.web.RequestMapper
import com.yahoo.bard.webservice.web.RequestValidationException
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest

import com.google.common.collect.Sets

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
    @Shared FilterOperation in_op = DefaultFilterOperation.in
    @Shared FilterOperation notin_op = notin

    @Shared ApiFilter mergedAFilter = new ApiFilter(
            filterDimension,
            dimensionField,
            in_op,
            (["1", "2"] as Set)
    )
    @Shared ApiFilter mergedABFilter = new ApiFilter(
            filterDimension,
            dimensionField,
            in_op,
            (["1", "2", "3"] as Set)
    )

    ApiFilter security1
    ApiFilter security2
    ApiFilter security3

    ApiFilter requestFilterInDimension
    ApiFilter requestFilterNotInDimension


    Map<Dimension, Set<ApiFilter>> requestFilters

    Set<ApiFilter> securitySetRoleA
    Set<ApiFilter> securitySetRoleB

    Map<String, Set<ApiFilter>> roles
    RequestMapper<DataApiRequest> next = Mock(RequestMapper)

    ResourceDictionaries dictionaries = Mock(ResourceDictionaries)


    RoleDimensionApiFilterRequestMapper mapper

    UserPrincipal userPrincipal = Mock(UserPrincipal)
    SecurityContext securityContext = Mock(SecurityContext)
    ContainerRequestContext context = Mock(ContainerRequestContext)

    def setupSpec() {
        filterDimension.getApiName() >> "filterDimension"
        nonFilterDimension.getApiName() >> "nonFilterDimension"
        dimensionField.toString() >> "fieldId"
    }
    def setup() {

        requestFilterNotInDimension = new ApiFilter(nonFilterDimension, dimensionField, notin_op, (["z"] as Set))

        security1 = new ApiFilter(filterDimension, dimensionField, in_op, (["1"] as Set))
        security2 = new ApiFilter(filterDimension, dimensionField, in_op, (["2"] as Set))
        security3 = new ApiFilter(filterDimension, dimensionField, in_op, (["3"] as Set))
        requestFilterInDimension = new ApiFilter(filterDimension, dimensionField, in_op, (["a", "1"] as Set))

        context.getSecurityContext() >> securityContext
        securitySetRoleA = [security1, security2] as Set
        securitySetRoleB = [security3] as Set

        roles = [A: securitySetRoleA, B: securitySetRoleB]

        mapper = new RoleDimensionApiFilterRequestMapper(
                dictionaries,
                filterDimension,
                roles,
                next
        )
        requestFilters = [(filterDimension)   : ([requestFilterInDimension] as Set),
                          (nonFilterDimension): ([requestFilterNotInDimension] as Set)]

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
        (["A"] as Set)           | new ApiFilter(filterDimension, dimensionField, in_op, (["1", "2"] as Set))
        (["B"] as Set)           | new ApiFilter(filterDimension, dimensionField, in_op, (["3"] as Set))
        (["A", "B"] as Set)      | new ApiFilter(filterDimension, dimensionField, in_op, (["1", "2", "3"] as Set))
        (["A", "B", "C"] as Set) | new ApiFilter(filterDimension, dimensionField, in_op, (["1", "2", "3"] as Set))
    }

    def "Test mergeSecurityFilters intersects on matching dimension and not other dimensions"() {
        setup:

        Set<ApiFilter> securityFilter = mapper.unionMergeFilterValues(securitySetRoleA.stream())

        Set<ApiFilter> expectedDimensions = Sets.union(Collections.singleton(requestFilterInDimension), securityFilter)
        Map<Dimension, Set<ApiFilter>> expected = [(filterDimension)   : expectedDimensions,
                                                   (nonFilterDimension): ([requestFilterNotInDimension] as Set)]

        expect:
        mapper.mergeSecurityFilters(requestFilters, securityFilter) == expected
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
    def "Filters #filterOne and #filterTwo  merge to #mergedFilters with expected canonical ordering"() {
        setup:
        ApiFilter one = new ApiFilter(*filterOne)
        ApiFilter two = new ApiFilter(*filterTwo)
        Set<ApiFilter> expected = mergedFilters.collect {
            new ApiFilter(*it)
        }

        expect:
        new ArrayList<>(RoleDimensionApiFilterRequestMapper.unionMergeFilterValues(Stream.of(one, two))) == new ArrayList<>(expected)

        where:
        filterOne | filterTwo | mergedFilters
        [filterDimension, dimensionField, notin, ["b", "a"] as Set] | [filterDimension, dimensionField, notin, ["c", "a"] as Set] |
                [[filterDimension, dimensionField, notin, ["a", "b", "c"] as Set]]
        [nonFilterDimension, dimensionField, notin, ["c", "a"] as Set] | [filterDimension, dimensionField, notin, ["b", "a"] as Set] |
                [
                        [filterDimension, dimensionField, notin, ["a", "b"] as List],
                        [nonFilterDimension, dimensionField, notin, ["a", "c"] as List],
                ] as LinkedHashSet
    }
}
