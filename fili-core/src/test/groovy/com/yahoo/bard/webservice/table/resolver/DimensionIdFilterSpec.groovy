// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.SearchProvider
import com.yahoo.bard.webservice.web.ApiFilter
import com.yahoo.bard.webservice.web.DefaultFilterOperation

import spock.lang.Specification
import spock.lang.Unroll

class DimensionIdFilterSpec extends Specification {

    def "Constructor builds the expected ApiFilter map"() {
        setup:
        def (Dimension dimension, _, ApiFilter expectedApiFilter, DimensionIdFilter dimensionIdFilter) = buildSimpleTestData()

        expect:
        dimensionIdFilter.dimensionKeySelectFilters == [(dimension): expectedApiFilter]
    }

    def "anyRowsMatch() throws IllegalArgumentException if dimension is not defined on the DimensionIdFilter"() {
        def (_, foo, bar, DimensionIdFilter dimensionIdFilter) = buildSimpleTestData()

        when:
        dimensionIdFilter.anyRowsMatch(Mock(Dimension), null)

        then:
        thrown(IllegalArgumentException)
    }

    def "anyRowsMatch() appends apiFilter from DimensionIdFilter to constraint filters and queries search provider"() {
        given: "A simple dimension id filter"
        def (Dimension dimension, SearchProvider searchProvider, ApiFilter dimensionIdApiFilter, DimensionIdFilter filter) = buildSimpleTestData()

        DimensionField keyField = dimension.getKey()

        and: "a search provider expecting concatenated api filters"
        ApiFilter constraintApiFilter = new ApiFilter(dimension, keyField, DefaultFilterOperation.in, ["a"] as Set)
        Set<ApiFilter> expectedSearchApiFilters = [dimensionIdApiFilter, constraintApiFilter] as HashSet

        1 * searchProvider.hasAnyRows(expectedSearchApiFilters) >> true

        expect: "Check that the expected values are submitted to the backing search provider"
        filter.anyRowsMatch(dimension, [constraintApiFilter] as Set)
    }


    @Unroll
    def "Does emptyConstraintOrAnyRows match return #expected with #description"() {
        given: "A constraint map with some constraints"
        def (Dimension dimension, SearchProvider searchProvider, Object foo, DimensionIdFilter filter) = buildSimpleTestData()

        Map<Dimension, Set<ApiFilter>> constraintMap = hasApiFilter ?
                [(dimension): (emptyApiFilter ? Collections.emptySet() : [Mock(ApiFilter)] as Set)] :
                [:]

        searchProvider.hasAnyRows(_) >> searchRowsExist

        expect: "filter matches when there are no constraints or they exist and return rows from search provider"
        filter.emptyConstraintOrAnyRows(dimension, constraintMap) == expected

        where:
        hasApiFilter  | emptyApiFilter | searchRowsExist | expected | description
        false         | null           | null            | true     | "no constraint"
        true          | true           | null            | true     | "empty constraint"
        true          | false          | true            | true     | "constraint with matching rows"
        true          | false          | false           | false    | "constrained dimension has no matching rows"
    }

    def buildSimpleTestData(Dimension dimension = null, Set<String> values = ["a", "b"] as Set) {
        SearchProvider searchProvider = Mock(SearchProvider)

        DimensionField keyField = Mock(DimensionField)

        if (dimension == null) {
            dimension = Mock(Dimension) {
                getApiName() >> "testDimension"
                getKey() >> keyField
                getSearchProvider() >> searchProvider
            }
        }

        ApiFilter expectedApiFilter = new ApiFilter(dimension, dimension.getKey(), DefaultFilterOperation.in, values)
        DimensionIdFilter dimensionIdFilter = new DimensionIdFilter([(dimension): values])

        [dimension, searchProvider, expectedApiFilter, dimensionIdFilter]
    }

    def "apply returns true as long as no dimensions constrain and return no rows"() {
        given: "A dimensionId filter with some filters"
        Set<String> values = ["a", "b"] as Set
        def (Dimension successDimension, SearchProvider hasRowsProvider, Object foo1, Object bar1) = buildSimpleTestData()
        def (Dimension failureDimension, SearchProvider hasNoRowsProvider, Object foo2, Object bar2) = buildSimpleTestData()
        def (Dimension nonFilterDimension, SearchProvider uncconfiguredSearch, Object foo3, Object bar3) = buildSimpleTestData()

        hasRowsProvider.hasAnyRows(_) >> true
        hasNoRowsProvider.hasAnyRows(_) >> false
        DimensionIdFilter dimensionIdFilter = new DimensionIdFilter([(successDimension): values, (failureDimension): values])

        and: "A constraint with some constraints"
        DataSourceConstraint constraint = Mock(DataSourceConstraint)
        Map<Dimension, ApiFilter> constraintMap = [:]
        constraint.getApiFilters() >> constraintMap

        if (constrainSuccess) {
            addFilterOnDimension(successDimension, constraintMap)
        }
        if (constrainFailure) {
            addFilterOnDimension(failureDimension, constraintMap)
        }
        if (constrainOther) {
            addFilterOnDimension(nonFilterDimension, constraintMap)
        }

        expect: "True as long as the dimension that doesnt return rows is not constrained"
        dimensionIdFilter.apply(constraint) == expected

        where:
        constrainFailure | constrainSuccess | constrainOther | expected
        true             | true             | true           | false
        true             | true             | false          | false
        true             | false            | true           | false
        true             | false            | false          | false
        false            | true             | true           | true
        false            | true             | false          | true
        false            | false            | true           | true
        false            | false            | false          | true
    }

    def addFilterOnDimension(Dimension dimension, Map<Dimension, Set<ApiFilter>> constraints, values = ["a"] as Set) {
        ApiFilter apiFilter = new ApiFilter(dimension, dimension.key, DefaultFilterOperation.in, values)
        constraints.put(dimension, [apiFilter] as Set)
    }
}
