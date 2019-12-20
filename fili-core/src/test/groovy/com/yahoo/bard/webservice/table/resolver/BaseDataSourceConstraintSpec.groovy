// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.web.filters.ApiFilters

import spock.lang.Specification
import spock.lang.Unroll

class BaseDataSourceConstraintSpec extends Specification {

    ApiFilters apiFilters = new ApiFilters()
    @Unroll
    def "#metricNames intersected with #other produces #newMetricNames"() {
        given:
        BaseDataSourceConstraint newDataSourceConstraint = new BaseDataSourceConstraint(
                [] as Set,
                [] as Set,
                [] as Set,
                metricNames as Set,
                [] as Set,
                [] as Set,
                [] as Set,
                apiFilters
        ).withMetricIntersection(other as Set)

        expect:
        newDataSourceConstraint.getMetricNames() == newMetricNames as Set

        where:
        metricNames                    | other                                         | newMetricNames
        []                             | []                                            | []
        ['metricName1', 'metricName2'] | ['metricName1']                               | ['metricName1']
        ['metricName1', 'metricName2'] | ['metricName1', 'metricName2']                | ['metricName1', 'metricName2']
        ['metricName1', 'metricName2'] | ['metricName1', 'metricName2', 'metricName3'] | ['metricName1', 'metricName2']
    }

    def "Dimension filtering properly removes filtered dimensions and all derived fields are properly rebuilt"() {
        setup:
        Dimension dim1 = Mock(Dimension)
        Dimension dim2 = Mock(Dimension)
        Dimension dim3 = Mock(Dimension)
        Dimension dim4 = Mock(Dimension)
        Dimension dim5 = Mock(Dimension)

        BaseDataSourceConstraint constraint =  new BaseDataSourceConstraint(
                [dim1, dim2] as Set,
                [dim3, dim4] as Set,
                [dim5] as Set,
                [] as Set,
                apiFilters
        )

        when:
        BaseDataSourceConstraint filtered = constraint.withDimensionFilter({![dim1, dim3].contains(it)})

        then:
        filtered.getRequestDimensions() == [dim2] as Set
        filtered.getFilterDimensions() == [dim4] as Set
        filtered.getMetricDimensions() == [dim5] as Set
        filtered.getAllDimensions() == [dim2, dim4, dim5] as Set
    }
}
