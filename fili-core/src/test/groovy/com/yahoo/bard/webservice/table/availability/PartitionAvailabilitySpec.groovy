// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.google.common.collect.Sets

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Function


/**
 * Test for partition availability behavior.
 */
class PartitionAvailabilitySpec extends Specification{

    PartitionAvailability partitionAvailability

    Availability availability1
    Availability availability2

    Column column1
    Column column2

    def setup() {
        availability1 = Mock(Availability)
        availability2 = Mock(Availability)
    }

    @Unroll
    def "getDataSourceNames returns #expected from #dataSourceNames1 and #dataSourceNames2"() {
        given:
        availability1.getDataSourceNames() >> Sets.newHashSet(dataSourceNames1.collect{it -> TableName.of(it)})
        availability2.getDataSourceNames() >> Sets.newHashSet(dataSourceNames2.collect{it -> TableName.of(it)})

        partitionAvailability = new PartitionAvailability(
                [availability1, availability2] as Set,
                Collections.emptySet(),
                {}
        )

        expect:
        partitionAvailability.getDataSourceNames() == Sets.newHashSet(expected.collect{it -> TableName.of(it)})

        where:
        dataSourceNames1       | dataSourceNames2 | expected
        []                     | []               | []
        ['source1']            | []               | ['source1']
        ['source1']            | ['source1']      | ['source1']
        ['source1', 'source2'] | ['source1']      | ['source1', 'source2']
    }

    @Unroll
    def "getAllAvailableIntervals returns merged intervals grouped by columns when two availabilities have #caseDescription"() {
        given:
        column1 = new Column('col1')
        column2 = new Column(column2Name)

        availability1.getAllAvailableIntervals() >> [
                (column1): intervallist1.collect{it -> new Interval(it)}
        ]
        availability2.getAllAvailableIntervals() >> [
                (column2): intervallist2.collect{it -> new Interval(it)}
        ]

        partitionAvailability = new PartitionAvailability(
                [availability1, availability2] as Set,
                [column1, column2] as Set,
                {}
        )

        expect:
        partitionAvailability.getAllAvailableIntervals() == expected.collectEntries{
            [(new Column(it.getKey())): it.getValue().collect{valueIt -> new Interval(valueIt)}]
        }

        where:
        column2Name | intervallist1              | intervallist2                                      | expected                                                           | caseDescription
        'col2'      | []                         | []                                                 | [:]                                                                | "different empty columns"
        'col1'      | []                         | []                                                 | [:]                                                                | "the same empty columns"
        'col2'      | ['2018-01-01/2018-02-01']  | []                                                 | [col1: ['2018-01-01/2018-02-01']]                                  | "different columns with one column being empty"
        'col1'      | ['2018-01-01/2018-02-01']  | []                                                 | [col1: ['2018-01-01/2018-02-01']]                                  | "the same columns with one column being empty"
        'col2'      | ['2018-01-01/2018-02-01']  | ['2018-01-01/2018-02-01']                          | [col1: ['2018-01-01/2018-02-01'], col2: ['2018-01-01/2018-02-01']] | "different columns with same list of intervals"
        'col1'      | ['2018-01-01/2018-02-01']  | ['2018-01-01/2018-02-01']                          | [col1: ['2018-01-01/2018-02-01']]                                  | "the same columns with same list of intervals"
        'col1'      | ['2018-01-01/2018-02-01']  | ['2018-02-01/2018-03-01', '2019-01-01/2019-02-01'] | [col1: ['2018-01-01/2018-03-01', '2019-01-01/2019-02-01']]         | "the same columns with intervals that need to be merged"
    }

    def "getAllAvailableIntervals returns intervals of only configured columns"() {
        given:
        column1 = new Column('col1')
        column2 = new Column('col2')

        availability1.getAllAvailableIntervals() >> [
                (column1): ['2018-01-01/2018-02-01']
        ]
        availability2.getAllAvailableIntervals() >> [
                (column2): ['2018-01-01/2018-02-01']
        ]

        partitionAvailability = new PartitionAvailability(
                [availability1, availability2] as Set,
                [column1] as Set,
                {}
        )

        when:
        Map<Column, List<Interval>> actual = partitionAvailability.getAllAvailableIntervals()

        then:
        actual.containsKey(column1)
        !actual.containsKey(column2)
    }

    def "getAvailableIntervals returns intervals in intersection when intervals have #reason"() {
        given:
        availability1.getAvailableIntervals(_ as DataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals1.collect{it -> new Interval(it)} as Set
        )
        availability2.getAvailableIntervals(_ as DataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals2.collect{it -> new Interval(it)} as Set
        )

        Function<DataSourceConstraint, Set<Availability>> partitionFunction = Mock(Function.class)
        partitionFunction.apply(_ as DataSourceConstraint) >> Sets.newHashSet(availability1, availability2)

        partitionAvailability = new PartitionAvailability(
                [availability1, availability2] as Set,
                Collections.emptySet(),
                partitionFunction
        )

        expect:
        partitionAvailability.getAvailableIntervals(Mock(DataSourceConstraint)) == new SimplifiedIntervalList(
                availableIntervals.collect{it -> new Interval(it)} as Set
        )

        where:
        availableIntervals1       | availableIntervals2       | availableIntervals        | reason
        []                        | []                        | []                        | "two empty intervals collections"
        ['2018-01-01/2018-02-01'] | []                        | []                        | "one interval collection being empty"
        ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-02-01'] | "full overlap (start/end, start/end)"
        ['2017-01-01/2017-02-01'] | ['2018-01-01/2018-02-01'] | []                        | "0 overlap (-10/-1, 0/10)"
        ['2017-01-01/2017-02-01'] | ['2017-02-01/2017-03-01'] | []                        | "0 overlap abutting (-10/0, 0/10)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-03-01'] | ['2017-01-15/2017-02-01'] | "partial front overlap (0/10, 5/15)"
        ['2017-01-01/2017-02-01'] | ['2016-10-01/2017-01-15'] | ['2017-01-01/2017-01-15'] | "partial back overlap (0/10, -5/5)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-02-01'] | ['2017-01-15/2017-02-01'] | "full front overlap (0/10, 5/10)"
        ['2017-01-01/2017-02-01'] | ['2017-01-01/2017-01-15'] | ['2017-01-01/2017-01-15'] | "full back overlap (0/10, 0/5)"
        ['2017-01-01/2017-02-01'] | ['2017-01-15/2017-01-25'] | ['2017-01-15/2017-01-25'] | "fully contain (0/10, 3/9)"

    }
}
