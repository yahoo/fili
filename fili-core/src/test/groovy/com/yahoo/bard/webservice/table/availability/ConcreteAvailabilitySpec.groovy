package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification

/**
 * Test for concrete availability behavior.
 */
class ConcreteAvailabilitySpec extends Specification{

    ConcreteAvailability concreteAvailability
    String columnPhysicalName1, columnPhysicalName2, columnPhysicalName3
    Interval interval1, interval2

    def setup() {

        columnPhysicalName1 = 'column_one'
        columnPhysicalName2 = 'column_two'
        columnPhysicalName3 = 'column_three'

        interval1 = new Interval('2000-01-01/2015-12-31')
        interval2 = new Interval('2010-01-01/2020-12-31')

        concreteAvailability = new ConcreteAvailability(
                TableName.of('table'),
                new TestDataSourceMetadataService([
                        (columnPhysicalName1): [interval1] as Set,
                        (columnPhysicalName2): [interval2] as Set,
                        'hidden_column'     : [interval1] as Set
                ])
        )
    }

    def "getAllAvailability returns all availabilities for all column in datasource metadata service"() {
        expect:
        concreteAvailability.getAllAvailableIntervals() == [
                (columnPhysicalName1): [interval1],
                (columnPhysicalName2): [interval2],
                'hidden_column'     : [interval1],
        ] as LinkedHashMap
    }

    def "getAvailableIntervals returns the intersection of the requested column available intervals"() {
        given:
        interval1 = new Interval(firstInterval)
        interval2 = new Interval(secondInterval)

        concreteAvailability = new ConcreteAvailability(
                TableName.of('table'),
                new TestDataSourceMetadataService([
                        (columnPhysicalName1): [interval1] as Set,
                        (columnPhysicalName2): [interval2] as Set
                ])
        )

        PhysicalDataSourceConstraint dataSourceConstraint = Mock(PhysicalDataSourceConstraint)
        dataSourceConstraint.getAllColumnPhysicalNames() >> [columnPhysicalName1, columnPhysicalName2]

        expect:
        concreteAvailability.getAvailableIntervals(dataSourceConstraint) == new SimplifiedIntervalList(
                expected.collect{new Interval(it)} as Set
        )

        where:
        firstInterval           | secondInterval          | expected                   | description
        '2017-01-01/2017-02-01' | '2017-01-01/2017-02-01' | ['2017-01-01/2017-02-01']  | "full overlap"
        '2017-01-01/2017-02-01' | '2018-01-01/2018-02-01' | []                         | "no overlap"
        '2017-01-01/2017-02-01' | '2017-02-01/2017-03-01' | []                         | "no overlap abutting"
        '2017-01-01/2017-02-01' | '2017-01-15/2017-03-01' | ['2017-01-15/2017-02-01']  | "front overlap"
        '2017-01-01/2017-02-01' | '2016-10-01/2017-01-15' | ['2017-01-01/2017-01-15']  | "back overlap"
        '2017-01-01/2017-02-01' | '2017-01-15/2017-02-01' | ['2017-01-15/2017-02-01']  | "full front overlap"
        '2017-01-01/2017-02-01' | '2017-01-01/2017-01-15' | ['2017-01-01/2017-01-15']  | "full back overlap"
        '2017-01-01/2017-02-01' | '2017-01-15/2017-01-25' | ['2017-01-15/2017-01-25']  | "fully contain"
    }

    def "getAvailableInterval returns empty interval if given column is not in data source metadata service"() {
        given:
        PhysicalDataSourceConstraint constraint = Mock(PhysicalDataSourceConstraint)
        constraint.getAllColumnPhysicalNames() >> ['ignored']

        expect:
        concreteAvailability.getAvailableIntervals(constraint) == new SimplifiedIntervalList()
    }

    def "getAvailableInterval returns empty interval if given empty column request"() {
        given:
        PhysicalDataSourceConstraint constraint = Mock(PhysicalDataSourceConstraint)
        constraint.getAllColumnPhysicalNames() >> []

        expect:
        concreteAvailability.getAvailableIntervals(constraint) == new SimplifiedIntervalList()
    }
}
