package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification

/**
 * Test for concrete availability behavior.
 */
class ConcreteAvailabilitySpec extends Specification{

    ConcreteAvailability concreteAvailability
    Column available1, available2
    Interval interval1, interval2

    def setup() {

        available1 = new Column('available1')
        available2 = new Column('available2')

        interval1 = new Interval('2010-01-01/2020-12-31')
        interval2 = new Interval('2017-01-01/2020-12-31')

        concreteAvailability = new ConcreteAvailability(
                TableName.of('table'),
                [available1, available2].toSet(),
                new TestDataSourceMetadataService([
                        (available1): [interval1].toSet(),
                        (available2): [interval2].toSet(),
                        (new Column('ignored')): [new Interval('2010-01-01/2500-12-31')].toSet()
                ])
        )
    }

    def "getAllAvailability returns the correct availabilities for each columns configured to the table"() {
        expect:
        concreteAvailability.getAllAvailableIntervals() == [
                (available1): [interval1].toSet(),
                (available2): [interval2].toSet()
        ] as LinkedHashMap
    }

    def "getAvailableIntervals returns the intersection of the requested column available intervals"() {
        given:
        DataSourceConstraint constraint = Mock(DataSourceConstraint)
        constraint.getAllColumnNames() >> ['available1', 'available2']

        expect:
        concreteAvailability.getAvailableIntervals(constraint) == new SimplifiedIntervalList([interval2])

    }

    def "getAvailableInterval returns empty interval if given column not configured"() {
        given:
        DataSourceConstraint constraint = Mock(DataSourceConstraint)
        constraint.getAllColumnNames() >> ['ignored']

        expect:
        concreteAvailability.getAvailableIntervals(constraint) == new SimplifiedIntervalList()
    }

    def "getAvailableInterval returns empty interval if given empty column request"() {
        given:
        DataSourceConstraint constraint = Mock(DataSourceConstraint)
        constraint.getAllColumnNames() >> []

        expect:
        concreteAvailability.getAvailableIntervals(constraint) == new SimplifiedIntervalList()
    }
}
