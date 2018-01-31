// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.web.filters.ApiFilters

import spock.lang.Specification
import spock.lang.Unroll

class DataSourceConstraintSpec extends Specification {

    ApiFilters apiFilters = new ApiFilters()
    @Unroll
    def "#metricNames intersected with #other produces #newMetricNames"() {
        given:
        DataSourceConstraint newDataSourceConstraint = new DataSourceConstraint(
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
}
