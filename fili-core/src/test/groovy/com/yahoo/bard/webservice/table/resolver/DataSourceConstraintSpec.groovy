// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.metric.MetricColumn
import spock.lang.Specification

class DataSourceConstraintSpec extends Specification {

    def "test withIntersectedColumnNames"() {
        given:
        DataSourceConstraint newDataSourceConstraint = new DataSourceConstraint(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                metricNames as Set,
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptyMap()
        ).withMetricIntersection(other.collect{it -> new MetricColumn(it)} as Set)

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
