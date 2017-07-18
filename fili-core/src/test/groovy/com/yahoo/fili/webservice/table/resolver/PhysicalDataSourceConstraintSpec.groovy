package com.yahoo.fili.webservice.table.resolver

import com.yahoo.fili.webservice.data.time.ZonedTimeGrain
import com.yahoo.fili.webservice.table.Column
import com.yahoo.fili.webservice.table.PhysicalTableSchema

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
                ['columnOne', 'columnTwo', 'columnThree', 'columnFour'] as Set,
                [:]
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
