// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
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
    Column column1, column2, column3
    Interval interval1, interval2

    def setup() {

        column1 = new Column('column1')
        column2 = new Column('column2')
        column3 = new Column('no_availability')

        interval1 = new Interval('2000-01-01/2015-12-31')
        interval2 = new Interval('2010-01-01/2020-12-31')

        concreteAvailability = new ConcreteAvailability(
                TableName.of('table'),
                [column1, column2, column3] as Set,
                new TestDataSourceMetadataService([
                        (column1): [interval1] as Set,
                        (column2): [interval2] as Set,
                        (new Column('ignored')): [new Interval('2010-01-01/2500-12-31')] as Set
                ])
        )
    }

    def "getAllAvailability returns the correct availabilities for each columns configured to the table"() {
        expect:
        concreteAvailability.getAllAvailableIntervals() == [
                (column1): [interval1],
                (column2): [interval2],
                (column3): [],
        ] as LinkedHashMap
    }

    def "getAvailableIntervals returns the intersection of the requested column available intervals"() {
        given:
        interval1 = new Interval(firstInterval)
        interval2 = new Interval(secondInterval)

        concreteAvailability = new ConcreteAvailability(
                TableName.of('table'),
                [column1, column2] as Set,
                new TestDataSourceMetadataService([
                        (column1): [interval1] as Set,
                        (column2): [interval2] as Set
                ])
        )

        DataSourceConstraint dataSourceConstraint = Mock(DataSourceConstraint)
        dataSourceConstraint.getAllColumnNames() >> ['column1', 'column2']

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

    def "getAvailableInterval returns empty interval if given column not configured to the table"() {
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
