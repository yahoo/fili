package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.PhysicalTableSchema

import spock.lang.Specification

/**
 * Test PhysicalDataSourceConstraint correctly translates and filters column physical names.
 */
class PhysicalDataSourceConstraintSpec extends Specification {

    DataSourceConstraint dataSourceConstraint
    PhysicalTableSchema physicalTableSchema
    PhysicalDataSourceConstraint physicalDataSourceConstraint

    def setup() {
        dataSourceConstraint =  new DataSourceConstraint(
                [] as Set,
                [] as Set,
                [] as Set,
                ['columnThree', 'columnFour', 'columnFive'] as Set,
                [] as Set,
                [] as Set,
                ['columnOne', 'columnTwo', 'columnThree', 'columnFour', 'columnFive'] as Set,
                [:]
        )

        physicalTableSchema = new PhysicalTableSchema(Mock(ZonedTimeGrain), [new Column('columnOne'), new Column('columnThree'), new Column('columnFour'), new Column('columnFive')], ['columnOne': 'column_one', 'columnTwo': 'column_two'])
        physicalDataSourceConstraint = new PhysicalDataSourceConstraint(dataSourceConstraint, physicalTableSchema)
    }

    def "physical data source constraint filters out columns not in the given schema"() {
        expect:
        !physicalDataSourceConstraint.getAllColumnPhysicalNames().contains('column_two')
    }

    def "physical data source constraint maps to physical name correctly even if mapping does not exist"() {
        expect:
        physicalDataSourceConstraint.getAllColumnPhysicalNames() == ['column_one', 'columnThree', 'columnFour', 'columnFive'] as Set
    }

    def "withMetricIntersection correctly intersects metricNames and allPhysicalColumnNames with the provided metrics"() {
        given:
        PhysicalDataSourceConstraint subConstraint = physicalDataSourceConstraint.withMetricIntersection(['columnThree', 'columnFour'] as Set)

        expect:
        subConstraint.getMetricNames() == ['columnThree', 'columnFour'] as Set
        subConstraint.withMetricIntersection(['columnThree', 'columnFour'] as Set).getAllColumnPhysicalNames() == ['column_one', 'columnThree', 'columnFour'] as Set
    }
}
