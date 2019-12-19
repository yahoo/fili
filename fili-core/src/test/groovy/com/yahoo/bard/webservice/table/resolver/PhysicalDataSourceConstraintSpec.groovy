// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.PhysicalTableSchema
import com.yahoo.bard.webservice.web.filters.ApiFilters

import spock.lang.Specification

/**
 * Test PhysicalDataSourceConstraint correctly translates and filters column physical names.
 */
class PhysicalDataSourceConstraintSpec extends Specification {

    BaseDataSourceConstraint dataSourceConstraint
    PhysicalTableSchema physicalTableSchema
    PhysicalDataSourceConstraint physicalDataSourceConstraint
    ApiFilters apiFilters = new ApiFilters()

    def setup() {
        dataSourceConstraint =  new BaseDataSourceConstraint(
                [] as Set,
                [] as Set,
                [] as Set,
                ['columnThree', 'columnFour', 'columnFive'] as Set,
                [] as Set,
                [] as Set,
                ['columnOne', 'columnTwo', 'columnThree', 'columnFour'] as Set,
                apiFilters
        )

        physicalTableSchema = new PhysicalTableSchema(Mock(ZonedTimeGrain), [new Column('columnOne'), new Column('columnTwo'), new Column('columnThree'), new Column('columnFour')], ['columnOne': 'column_one', 'columnTwo': 'column_two'])
        physicalDataSourceConstraint = new PhysicalDataSourceConstraint(dataSourceConstraint, physicalTableSchema)
    }

    def "physical data source constraint maps to physical name correctly even if mapping does not exist"() {
        expect:
        physicalDataSourceConstraint.getAllColumnPhysicalNames() == ['column_one', 'column_two', 'columnThree', 'columnFour'] as Set
    }

    def "withMetricIntersection correctly intersects metricNames and allPhysicalColumnNames with the provided metrics"() {
        given:
        PhysicalDataSourceConstraint subConstraint = physicalDataSourceConstraint.withMetricIntersection(['columnFour'] as Set)

        expect:
        subConstraint.getMetricNames() == ['columnFour'] as Set
        subConstraint.withMetricIntersection(['columnFour'] as Set).getAllColumnPhysicalNames() == ['column_one', 'column_two', 'columnFour'] as Set
    }
}
