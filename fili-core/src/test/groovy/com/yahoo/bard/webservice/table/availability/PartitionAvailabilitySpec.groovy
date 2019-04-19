// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import static org.joda.time.DateTimeZone.UTC
import static org.joda.time.DateTimeZone.getDefault
import static org.joda.time.DateTimeZone.setDefault

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.data.config.names.TableName
import com.yahoo.bard.webservice.table.Column
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import com.google.common.collect.Sets

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
/**
 * Test for partition availability behavior.
 */
class PartitionAvailabilitySpec extends Specification {

    static DateTimeZone originalTimeZone
    static {
        originalTimeZone = DateTimeZone.getDefault()
        DateTimeZone.setDefault(DateTimeZone.UTC)
    }

    public static final String DISTANT_PAST_STR = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm").print(Availability.DISTANT_PAST)
    public static final String FAR_FUTURE_STR = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm").print(Availability.FAR_FUTURE)

    public static final String SOURCE1 = 'source1'
    public static final String SOURCE2 = 'source2'
    PartitionAvailability partitionAvailability

    Availability availability1
    Availability availability2

    static DateTimeZone dateTimeZone
    {
        dateTimeZone = getDefault();
        setDefault(UTC)
    }

    @Shared SimplifiedIntervalList midInterval = new SimplifiedIntervalList([new Interval('2012/2015')])
    @Shared SimplifiedIntervalList earlyInterval = new SimplifiedIntervalList([new Interval('2010/2014')])
    @Shared SimplifiedIntervalList lateInterval = new SimplifiedIntervalList([new Interval('2013/2016')])

    Column column1
    Column column2

    DateTime startDate_1
    DateTime startDate_2

    DateTime endDate_1
    DateTime endDate_2

    def cleanupSpec() {
        DateTimeZone.setDefault(originalTimeZone)
    }

    def setup() {
        availability1 = Mock(Availability)
        availability2 = Mock(Availability)

        availability1.getDataSourceNames() >> ([TableName.of(SOURCE1)] as Set)
        availability2.getDataSourceNames() >> ([TableName.of(SOURCE2)] as Set)

        startDate_1 = null
        startDate_2 = null

        endDate_1 = null
        endDate_2 = null

        // groovy mocks DO NOT inherit default methods, so we MUST mock the getExpectedStart/EndDate methods for this to work
        availability1.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> { Optional.ofNullable(startDate_1) }
        availability2.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> { Optional.ofNullable(startDate_2) }

        availability1.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> { Optional.ofNullable(endDate_1) }
        availability2.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> { Optional.ofNullable(endDate_2) }
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

    def "getDataSourceNames(constraint) returns only datasource names that are actually needed"() {
        given:
        DataSourceName name1 = DataSourceName.of("datasource1")
        DataSourceName name2 = DataSourceName.of("datasource2")

        availability1 = Mock(Availability)
        availability2 = Mock(Availability)

        availability1.getDataSourceNames() >> ([name1] as Set)
        availability2.getDataSourceNames() >> ([name2] as Set)

        availability1.getDataSourceNames(_ as DataSourceConstraint) >> ([name1] as Set)
        availability2.getDataSourceNames(_ as DataSourceConstraint) >> ([name2] as Set)

        DataSourceFilter partition1 = Mock(DataSourceFilter)
        DataSourceFilter partition2 = Mock(DataSourceFilter)

        partition1.apply(_ as DataSourceConstraint) >> true
        partition2.apply(_ as DataSourceConstraint) >> false

        Map<Availability, DataSourceFilter> partitionMap = [(availability1): partition1, (availability2) : partition2]

        partitionAvailability = new PartitionAvailability(partitionMap)

        expect:
        partitionAvailability.getDataSourceNames(Mock(PhysicalDataSourceConstraint)) == [name1] as Set
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
        early.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> Optional.empty()
        early.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> Optional.empty()

        Availability mid = Mock(Availability)
        mid.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> midInterval
        mid.getDataSourceNames() >> ([TableName.of('mid')] as Set)
        mid.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> Optional.empty()
        mid.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> Optional.empty()

        Availability late = Mock(Availability)
        late.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> lateInterval
        late.getDataSourceNames() >> ([TableName.of('late')] as Set)
        late.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> Optional.empty()
        late.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> Optional.empty()

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

    @Unroll
    def "test missing intervals properly created for #interval interval and #desc"() {
        given:
        startDate_1 = start
        endDate_1 = end

        SimplifiedIntervalList actualAvailability = new SimplifiedIntervalList(availableIntervalsToTest.collect({it -> new Interval(it)}))
        availability1.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> { actualAvailability }

        SimplifiedIntervalList expectedMissing = new SimplifiedIntervalList(expectedMissingIntervals.collect({it -> new Interval(it)}))

        partitionAvailability = new PartitionAvailability([(availability1): {true} as DataSourceFilter] as Map)

        expect:
        SimplifiedIntervalList result = partitionAvailability.getBoundedMissingIntervalsWithConstraint(availability1, Mock(PhysicalDataSourceConstraint))
        result == expectedMissing

        where:
        start                           |   end                         |   availableIntervalsToTest                                ||   expectedMissingIntervals                                                                                               |   interval                                        |   desc
        null                            |   null                        |   ["2017-01-01/2018-01-01"]                               ||   ["${DISTANT_PAST_STR}/2017-01-01".toString(), "2018-01-01/${FAR_FUTURE_STR}".toString()]                               |   "unbroken"                                      |   "no expected start nor end"
        null                            |   null                        |   ["2014-01-01/2015-01-01", "2016-01-01/2017-01-01"]      ||   ["${DISTANT_PAST_STR}/2014-01-01".toString(), "2015-01-01/2016-01-01", "2017-01-01/${FAR_FUTURE_STR}".toString()]      |   "two separate"                                  |   "no expected start nor end"
        null                            |   new DateTime(2018,1,1,0,0)  |   ["2017-01-01/2018-01-01"]                               ||   ["${DISTANT_PAST_STR}/2017-01-01".toString()]                                                                          |   "unbroken"                                      |   "concrete end, no expected start"
        null                            |   new DateTime(2018,1,1,0,0)  |   ["2017-01-01/2020-01-01"]                               ||   ["${DISTANT_PAST_STR}/2017-01-01".toString()]                                                                          |   "unbroken but outside end"                      |   "concrete end, no expected start"
        null                            |   new DateTime(2018,1,1,0,0)  |   ["2017-01-01/2018-01-01", "2019-01-01/2020-01-01"]      ||   ["${DISTANT_PAST_STR}/2017-01-01".toString()]                                                                          |   "one before end, one after end"                 |   "concrete end, no expected start"
        null                            |   new DateTime(2018,1,1,0,0)  |   ["2013-01-01/2014-01-01", "2017-01-01/2018-01-01"]      ||   ["${DISTANT_PAST_STR}/2013-01-01".toString(), "2014-01-01/2017-01-01"]                                                 |   "two separate before end"                       |   "concrete end, no expected start"
        new DateTime(2014,1,1,0,0)      |   null                        |   ["2014-01-01/2015-01-01"]                               ||   ["2015-01-01/${FAR_FUTURE_STR}".toString()]                                                                            |   "unbroken"                                      |   "concrete beginning, no expected end"
        new DateTime(2014,1,1,0,0)      |   null                        |   ["2012-01-01/2013-01-01", "2014-01-01/2015-01-01"]      ||   ["2015-01-01/${FAR_FUTURE_STR}".toString()]                                                                            |   "two separate, one before start"                |   "concrete beginning, no expected end"
        new DateTime(2014,1,1,0,0)      |   null                        |   ["2012-01-01/2015-01-01", "2017-01-01/2018-01-01"]      ||   ["2015-01-01/2017-01-01", "2018-01-01/${FAR_FUTURE_STR}".toString()]                                                   |   "two separate, after start"                     |   "concrete beginning, no expected end"
        new DateTime(2014,1,1,0,0)      |   new DateTime(2018,1,1,0,0)  |   ["2014-01-01/2018-01-01"]                               ||   []                                                                                                                     |   "unbroken"                                      |   "concrete beginning and end"
        new DateTime(2014,1,1,0,0)      |   new DateTime(2018,1,1,0,0)  |   ["2012-01-01/2014-01-01", "2016-01-01/2020-01-01"]      ||   ["2014-01-01/2016-01-01"]                                                                                              |   "two separate, one end of each outside range"   |   "concrete beginning and end"
        new DateTime(2014,1,1,0,0)      |   new DateTime(2018,1,1,0,0)  |   ["2014-01-01/2016-01-01"]                               ||   ["2016-01-01/2018-01-01"]                                                                                              |   "unbroken, from start to middle"                |   "concrete beginning and end"
        new DateTime(2014,1,1,0,0)      |   new DateTime(2018,1,1,0,0)  |   ["2016-01-01/2018-01-01"]                               ||   ["2014-01-01/2016-01-01"]                                                                                              |   "unbroken, from middle to end"                  |   "concrete beginning and end"
        new DateTime(2014,1,1,0,0)      |   new DateTime(2018,1,1,0,0)  |   ["2015-01-01/2017-01-01"]                               ||   ["2014-01-01/2015-01-01", "2017-01-01/2018-01-01"]                                                                     |   "unbroken, after start before end"              |   "concrete beginning and end"
        new DateTime(2014,1,1,0,0)      |   new DateTime(2018,1,1,0,0)  |   ["2015-01-01/2015-06-01", "2016-01-01/2016-06-01"]      ||   ["2014-01-01/2015-01-01", "2015-06-01/2016-01-01", "2016-06-01/2018-01-01"]                                            |   "two separate, both after start before end"     |   "concrete beginning and end"
    }

    @Unroll
    def "partition availability calculated correctly when #desc"() {
        given:
        startDate_1 = start_1
        endDate_1 = end_1

        startDate_2 = start_2
        endDate_2 = end_2

        SimplifiedIntervalList actualAvailability_1 = new SimplifiedIntervalList(actual_1.collect({it -> new Interval(it)}))
        SimplifiedIntervalList actualAvailability_2 = new SimplifiedIntervalList(actual_2.collect({it -> new Interval(it)}))
        availability1.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> { actualAvailability_1 }
        availability2.getAvailableIntervals(_ as PhysicalDataSourceConstraint) >> { actualAvailability_2 }

        partitionAvailability = new PartitionAvailability([
                (availability1): {true} as DataSourceFilter,
                (availability2): {true} as DataSourceFilter
        ] as Map)

        SimplifiedIntervalList expectedAvailability = new SimplifiedIntervalList(expected.collect({it -> new Interval(it)}))

        expect:
        SimplifiedIntervalList result = partitionAvailability.getAvailableIntervals(Mock(PhysicalDataSourceConstraint))
        result == expectedAvailability

        where:
        start_1                     |   end_1                       |   start_2                     |   end_2                       |   actual_1                    |   actual_2                                            |   expected                                            |   desc
        null                        |   null                        |   null                        |   null                        |   ["2016-01-01/2017-01-01"]   |   ["2016-01-01/2017-01-01"]                           |   ["2016-01-01/2017-01-01"]                           |   "both subparts no bounds and same availability"
        null                        |   new DateTime(2017,1,1,0,0)  |   new DateTime(2017,1,1,0,0)  |   null                        |   ["2016-01-01/2017-01-01"]   |   ["2017-01-01/2018-01-01"]                           |   ["2016-01-01/2018-01-01"]                           |   "subparts are abutting, and ends not abutting have no expected start or end respectively"
        null                        |   new DateTime(2017,6,1,0,0)  |   new DateTime(2016,6,1,0,0)  |   null                        |   ["2016-01-01/2017-06-01"]   |   ["2016-06-01/2018-01-01"]                           |   ["2016-01-01/2018-01-01"]                           |   "subparts are partially overlapping, and ends not overlapping have no expected start or end respectively"
        null                        |   new DateTime(2018,1,1,0,0)  |   new DateTime(2016,1,1,0,0)  |   null                        |   ["2016-01-01/2017-06-01"]   |   ["2016-06-01/2018-01-01"]                           |   ["2016-06-01/2017-06-01"]                           |   "subparts are partially overlapping, with missing data on non overlapping intervals of each other"
        null                        |   new DateTime(2018,1,1,0,0)  |   new DateTime(2016,1,1,0,0)  |   null                        |   ["2016-01-01/2017-01-01"]   |   ["2017-01-01/2018-01-01"]                           |   []                                                  |   "subparts are abutting, with missing completely overlapping the other part"
        null                        |   new DateTime(2016,6,1,0,0)  |   new DateTime(2017,6,1,0,0)  |   null                        |   ["2016-01-01/2016-06-01"]   |   ["2017-06-01/2018-01-01"]                           |   ["2016-01-01/2016-06-01", "2017-06-01/2018-01-01"]  |   "subparts are separate and do not have any missing overlapping with each other's availability (1)"
        null                        |   new DateTime(2017,6,1,0,0)  |   new DateTime(2017,6,1,0,0)  |   null                        |   ["2016-01-01/2016-06-01"]   |   ["2017-06-01/2018-01-01"]                           |   ["2016-01-01/2016-06-01", "2017-06-01/2018-01-01"]  |   "subparts are separate and do not have any missing overlapping with each other's availability (2)"
        new DateTime(2016,1,1,0,0)  |   new DateTime(2017,6,1,0,0)  |   new DateTime(2017,6,1,0,0)  |   new DateTime(2018,1,1,0,0)  |   ["2016-01-01/2016-06-01"]   |   ["2017-06-01/2018-01-01"]                           |   ["2016-01-01/2016-06-01", "2017-06-01/2018-01-01"]  |   "subparts are separate and do not have any missing overlapping with each other's availability (3)"
        new DateTime(2016,1,1,0,0)  |   new DateTime(2018,1,1,0,0)  |   new DateTime(2016,1,1,0,0)  |   new DateTime(2018,1,1,0,0)  |   ["2016-01-01/2018-01-01"]   |   []                                                  |   []                                                  |   "both subparts have expected start and end, but one part is completely missing and is same as availabile part"
        new DateTime(2016,1,1,0,0)  |   new DateTime(2018,1,1,0,0)  |   new DateTime(2015,1,1,0,0)  |   new DateTime(2020,1,1,0,0)  |   ["2016-01-01/2018-01-01"]   |   []                                                  |   []                                                  |   "both subparts have expected start and end, but one part is completely missing and is larger than available part"
        new DateTime(2016,1,1,0,0)  |   new DateTime(2018,1,1,0,0)  |   new DateTime(2016,6,1,0,0)  |   new DateTime(2017,6,1,0,0)  |   ["2016-01-01/2018-01-01"]   |   []                                                  |   ["2016-01-01/2016-06-01", "2017-06-01/2018-01-01"]  |   "both subparts have expected start and end, but one part is completely missing and is contained by available part"
        new DateTime(2016,1,1,0,0)  |   new DateTime(2017,1,1,0,0)  |   new DateTime(2016,1,1,0,0)  |   new DateTime(2017,1,1,0,0)  |   ["2016-01-01/2017-01-01"]   |   ["2016-01-01/2017-01-01"]                           |   ["2016-01-01/2017-01-01"]                           |   "both subparts same availability, availability equivalent to bounds, no missing"
        new DateTime(2016,1,1,0,0)  |   new DateTime(2018,1,1,0,0)  |   new DateTime(2016,1,1,0,0)  |   new DateTime(2018,1,1,0,0)  |   ["2016-01-01/2018-01-01"]   |   ["2016-06-01/2016-09-01", "2017-06-01/2017-09-01"]  |   ["2016-06-01/2016-09-01", "2017-06-01/2017-09-01"]  |   "both subparts have expected start and end, but one part has fragmented availability and is contained by available part"
    }
}
