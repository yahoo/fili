// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.metadata.TestDataSourceMetadataService
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class PermissiveAvailabilitySpec extends Specification  {

    PermissiveAvailability permissiveAvailability
    TableName tableName
    String column1
    String column2
    Interval interval1
    Interval interval2

    def setup() {
        tableName = TableName.of('table')

        column1 = 'column_one'
        column2 = 'column_two'

        interval1 = new Interval('2017-01-01/2017-02-01')
        interval2 = new Interval('2018-01-01/2018-02-01')

        permissiveAvailability = new PermissiveAvailability(
                tableName,
                new TestDataSourceMetadataService([
                        (column1): [interval1] as Set,
                        (column2): [interval2] as Set,
                        'ignored_column': [interval1] as Set
                ])
        )
    }

    @Unroll
    def "getAvailableIntervals returns the union of requested columns when available intervals have #reason"() {
        given:
        interval1 = new Interval(firstInterval)
        interval2 = new Interval(secondInterval)

        permissiveAvailability = new PermissiveAvailability(
                tableName,
                new TestDataSourceMetadataService([
                        (column1): [interval1] as Set,
                        (column2): [interval2] as Set
                ])
        )

        PhysicalDataSourceConstraint dataSourceConstraint = Mock(PhysicalDataSourceConstraint)
        dataSourceConstraint.getAllColumnPhysicalNames() >> [column1, column2]

        expect:
        permissiveAvailability.getAvailableIntervals(dataSourceConstraint) == new SimplifiedIntervalList(
                expected.collect{new Interval(it)} as Set
        )

        where:
        firstInterval           | secondInterval          | expected                                           | reason
        '2017-01-01/2017-02-01' | '2017-01-01/2017-02-01' | ['2017-01-01/2017-02-01']                          | "full overlap (start/end, start/end)"
        '2017-01-01/2017-02-01' | '2018-01-01/2018-02-01' | ['2017-01-01/2017-02-01', '2018-01-01/2018-02-01'] | "0 overlap (-10/-1, 0/10)"
        '2017-01-01/2017-02-01' | '2017-02-01/2017-03-01' | ['2017-01-01/2017-03-01']                          | "0 overlap abutting (-10/0, 0/10)"
        '2017-01-01/2017-02-01' | '2017-01-15/2017-03-01' | ['2017-01-01/2017-03-01']                          | "partial front overlap (0/10, 5/15)"
        '2017-01-01/2017-02-01' | '2016-10-01/2017-01-15' | ['2016-10-01/2017-02-01']                          | "partial back overlap (0/10, -5/5)"
        '2017-01-01/2017-02-01' | '2017-01-15/2017-02-01' | ['2017-01-01/2017-02-01']                          | "full front overlap (0/10, 5/10)"
        '2017-01-01/2017-02-01' | '2017-01-01/2017-01-15' | ['2017-01-01/2017-02-01']                          | "full back overlap (0/10, 0/5)"
        '2017-01-01/2017-02-01' | '2017-01-15/2017-01-25' | ['2017-01-01/2017-02-01']                          | "fully contain (0/10, 3/9)"
    }

    def "getAllAvailability returns the correct availabilities for each column in datasource metadata service"() {
        expect:
        permissiveAvailability.getAllAvailableIntervals() == [
                (column1): [interval1],
                (column2): [interval2],
                'ignored_column': [interval1],
        ] as LinkedHashMap
    }
}
