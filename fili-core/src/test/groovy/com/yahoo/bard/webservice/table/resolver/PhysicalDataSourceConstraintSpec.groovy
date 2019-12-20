// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.dimension.Dimension
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

    String dim1Name
    String dim2Name
    String dim3Name
    String dim4Name

    Dimension dim1
    Dimension dim2
    Dimension dim3
    Dimension dim4


    def setup() {
        dim1Name = "dim1"
        dim1 = Mock(Dimension) {getApiName() >> {dim1Name}}

        dim2Name = "dim2"
        dim2 = Mock(Dimension) {getApiName() >> {dim2Name}}

        dim3Name = "dim3"
        dim3 = Mock(Dimension) {getApiName() >> {dim3Name}}

        dim4Name = "dim4"
        dim4 = Mock(Dimension) {getApiName() >> {dim4Name}}

        dataSourceConstraint =  new BaseDataSourceConstraint(
                [dim1, dim2] as Set,
                [dim3, dim4] as Set,
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

    def "withDimensionFilter properly removes filtered dimensions"() {
        given:
        PhysicalDataSourceConstraint filtered = physicalDataSourceConstraint.withDimensionFilter({[dim1Name, dim3Name].contains(it.getApiName())})

        expect:
        filtered.getAllDimensionNames() == [dim1Name, dim3Name] as Set
        filtered.getRequestDimensions() == [dim1] as Set
        filtered.getFilterDimensions() == [dim3] as Set
        filtered.getMetricDimensions() == [] as Set
    }
}
