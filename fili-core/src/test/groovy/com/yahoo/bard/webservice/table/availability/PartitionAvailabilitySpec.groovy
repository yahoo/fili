// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.data.time.ZonedTimeGrainSpec
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.google.common.collect.Sets

import org.joda.time.Interval

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
/**
 * Test for partition availability behavior.
 */
class PartitionAvailabilitySpec extends Specification{

    public static final String SOURCE1 = 'source1'
    public static final String SOURCE2 = 'source2'
    PartitionAvailability partitionAvailability

    ZonedTimeGrain testTimeGrain = ZonedTimeGrainSpec.DAY_UTC

    Availability availability1
    Availability availability2

    @Shared SimplifiedIntervalList midInterval = new SimplifiedIntervalList([new Interval('2012/2015')])
    @Shared SimplifiedIntervalList earlyInterval = new SimplifiedIntervalList([new Interval('2010/2014')])
    @Shared SimplifiedIntervalList lateInterval = new SimplifiedIntervalList([new Interval('2013/2016')])

    Column column1
    Column column2

    def setup() {
        availability1 = Mock(Availability)
        availability2 = Mock(Availability)

        availability1.getDataSourceNames() >> ([TableName.of(SOURCE1)] as Set)
        availability2.getDataSourceNames() >> ([TableName.of(SOURCE2)] as Set)

    }

    @Unroll
    def "getDataSourceNames returns #expected from #dataSourceNames1 and #dataSourceNames2"() {
        given:
        availability1 = Mock(Availability)
        availability2 = Mock(Availability)

        availability1.getDataSourceNames() >> (names1.collect{TableName.of(it)} as Set)
        availability2.getDataSourceNames() >> (names2.collect{TableName.of(it)} as Set)

        partitionAvailability = new PartitionAvailability(
                [(availability1): {} as DataSourceFilter, (availability2): {} as DataSourceFilter]
        )

        expect:
        partitionAvailability.getDataSourceNames() == Sets.newHashSet(expected.collect{it -> TableName.of(it)})

        where:
        names1             | names2    | expected
        []                 | []        | []
        [SOURCE1]          | []        | [SOURCE1]
        [SOURCE1]          | [SOURCE1] | [SOURCE1]
        [SOURCE1, SOURCE2] | [SOURCE1] | [SOURCE1, SOURCE2]
    }

    @Unroll
    def "getAllAvailableIntervals returns merged intervals grouped by columns when two availabilities have #caseDescription (metadata map merge)"() {
        given:
        column1 = new Column('col1')
        column2 = new Column(column2Name)

        availability1.getAllAvailableIntervals() >> [(column1): intervallist1.collect{new Interval(it)} as SimplifiedIntervalList]
        availability2.getAllAvailableIntervals() >> [(column2): intervallist2.collect{new Interval(it)} as SimplifiedIntervalList]

        partitionAvailability = new PartitionAvailability(
                [(availability1): {true}, (availability2): {true}]
        )

        expect:
        partitionAvailability.getAllAvailableIntervals() == expected.collectEntries{
            [(new Column(it.getKey())): it.getValue().collect{valueIt -> new Interval(valueIt)}]
        }

        where:
        column2Name | intervallist1              | intervallist2                                      | expected                                                           | caseDescription
        'col2'      | []                         | []                                                 | [col1:[], col2: []]                                                | "different empty columns"
        'col1'      | []                         | []                                                 | [col1:[]]                                                          | "the same empty columns"
        'col2'      | ['2018-01-01/2018-02-01']  | []                                                 | [col1: ['2018-01-01/2018-02-01'], col2: []]                        | "different columns with one column being empty"
        'col1'      | ['2018-01-01/2018-02-01']  | []                                                 | [col1: ['2018-01-01/2018-02-01']]                                  | "the same columns with one column being empty"
        'col2'      | ['2018-01-01/2018-02-01']  | ['2018-01-01/2018-02-01']                          | [col1: ['2018-01-01/2018-02-01'], col2: ['2018-01-01/2018-02-01']] | "different columns with same list of intervals"
        'col1'      | ['2018-01-01/2018-02-01']  | ['2018-01-01/2018-02-01']                          | [col1: ['2018-01-01/2018-02-01']]                                  | "the same columns with same list of intervals"
        'col1'      | ['2018-01-01/2018-02-01']  | ['2018-02-01/2018-03-01', '2019-01-01/2019-02-01'] | [col1: ['2018-01-01/2018-03-01', '2019-01-01/2019-02-01']]         | "the same columns with intervals that need to be merged"
    }

    @Unroll
    def "getAvailableIntervals returns intervals in intersection when intervals have #reason (simple time merge)"() {
        given:

        availability1.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals1.collect{it -> new Interval(it)} as Set
        )
        availability2.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> new SimplifiedIntervalList(
                availableIntervals2.collect{it -> new Interval(it)} as Set
        )

        partitionAvailability = new PartitionAvailability(
                [(availability1): {true} as DataSourceFilter, (availability2): {true} as DataSourceFilter]
        )

        expect:
        partitionAvailability.getAvailableIntervals(Mock(PhysicalDataSourceConstraint)) == new SimplifiedIntervalList(
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

    @Unroll
    def "getAvailableIntervals with partitions #partitionsImpacted returns #expectedIntervals (gated time merge)"() {
        given:
        Availability early = Mock(Availability)
        early.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> earlyInterval
        early.getDataSourceNames() >> ([TableName.of('early')] as Set)

        Availability mid = Mock(Availability)
        mid.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> midInterval
        mid.getDataSourceNames() >> ([TableName.of('mid')] as Set)

        Availability late = Mock(Availability)
        late.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> lateInterval
        late.getDataSourceNames() >> ([TableName.of('late')] as Set)

        Set<Availability> availabilities = [early, mid, late].findAll { partitionsImpacted.containsAll(it.dataSourceNames.collect() {it.asName()}) } as Set
        PhysicalDataSourceConstraint constraint = Mock(PhysicalDataSourceConstraint)

        partitionAvailability = new PartitionAvailability(
                [
                        (early): { partitionsImpacted.contains('early') } as DataSourceFilter,
                        (mid)  : { partitionsImpacted.contains('mid') } as DataSourceFilter,
                        (late) : { partitionsImpacted.contains('late') } as DataSourceFilter
                ]
        )

        expect:
        partitionAvailability.getAvailableIntervals(constraint) == expectedIntervals

        where:
        partitionsImpacted       | expectedIntervals
        ['mid']                  | midInterval
        ['early']                | earlyInterval
        ['late']                 | lateInterval
        ['early', 'mid']         | earlyInterval.intersect(midInterval)
        ['mid', 'late']          | midInterval.intersect(lateInterval)
        ['early', 'late']        | earlyInterval.intersect(lateInterval)
        ['early', 'mid', 'late'] | earlyInterval.intersect(lateInterval).intersect(midInterval)
    }
}
