// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class LeftPureUnionAvailabilitySpec extends Specification {
    Availability representativeAvailability
    Availability nonRepresentativeAvailability

    String metric

    LeftPureUnionAvailability leftPureUnionAvailability

    Map<Availability, Set<String>> availabilitiesToMetricNames

    def setup() {
        representativeAvailability = Mock(Availability)
        nonRepresentativeAvailability = Mock(Availability)

        representativeAvailability.dataSourceNames >> ([DataSourceName.of('source1')] as Set)
        nonRepresentativeAvailability.dataSourceNames >> ([DataSourceName.of('source2')] as Set)

        metric = 'metric'

        availabilitiesToMetricNames = [
                (representativeAvailability): [metric] as Set,
                (nonRepresentativeAvailability): [metric] as Set
        ]

        leftPureUnionAvailability = new LeftPureUnionAvailability(
                representativeAvailability,
                [representativeAvailability, nonRepresentativeAvailability] as Set
        )
    }

    /**
     * Returns a list of time intervals by converting a collection of their string representations.
     * <p>
     * The String formats are described by {@link org.joda.time.format.ISODateTimeFormat#dateTimeParser()} and
     * {@link org.joda.time.format.ISOPeriodFormat#standard()}, and may be 'datetime/datetime', 'datetime/period' or
     * 'period/datetime'.
     *
     * @param intervals  The collection of string time-intervals
     *
     * @return the list of time interval objects
     */
    static List<Interval> parseIntervals(Collection<String> intervals) {
        intervals.collect {it -> new Interval(it)}
    }

    def "Without constraint, Availability returns immutable datasources of all participating Availabilities"() {
        when: "datasources are requested"
        Set<DataSourceName> dataSourceNames = leftPureUnionAvailability.getDataSourceNames()

        then: "datasources are only associated with the representative availability"
        dataSourceNames.collect {it -> it.asName()} == ["source1", "source2"]

        when: "when we try to mutate the datasources"
        dataSourceNames.add(DataSourceName.of("hack"))

        then: "error is thrown"
        Exception exception = thrown()
        exception instanceof UnsupportedOperationException
    }

    def "With constraint, Availability returns constrained immutable datasources of all participating Availabilities"() {
        given: "a constraint"
        DataSourceConstraint constraint = Mock(DataSourceConstraint)

        and: "the constraint filters out all datasources in representative availability but keeps a datasource in non-representative availability"
        representativeAvailability.getDataSourceNames(constraint) >> ([DataSourceName.of("constrainedSource1")] as Set)
        nonRepresentativeAvailability.getDataSourceNames(constraint) >> ([DataSourceName.of("constrainedSource2")] as Set)

        when: "datasources are requested"
        Set<DataSourceName> dataSourceNames = leftPureUnionAvailability.getDataSourceNames(constraint)

        then: "datasources are only associated with the representative availability"
        dataSourceNames.collect {it -> it.asName()} == ["constrainedSource1", "constrainedSource2"]

        when: "when we try to mutate the datasources"
        dataSourceNames.add(DataSourceName.of("hack"))

        then: "error is thrown"
        Exception exception = thrown()
        exception instanceof UnsupportedOperationException
    }

    @Unroll
    def "Availabilities by columns reflects only representative availability when rep. and non-rep. availabilities have #intervals"() {
        setup: "representative availability reflects a set of available intervals"
        representativeAvailability.getAllAvailableIntervals() >> [
                (metric): SimplifiedIntervalList.simplifyIntervals(
                        parseIntervals(representativeIntervals)
                )
        ]

        and: "non-representative availability reflects another set of available intervals"
        nonRepresentativeAvailability.getAllAvailableIntervals() >> [
                (metric): SimplifiedIntervalList.simplifyIntervals(
                        parseIntervals(nonRepresentativeIntervals)
                )
        ]

        expect: "pure left union availability always reflects the availabilities from the representative availability"
        leftPureUnionAvailability.getAllAvailableIntervals() == [
                (metric): SimplifiedIntervalList.simplifyIntervals(
                        parseIntervals(representativeIntervals)
                )
        ]

        where:
        representativeIntervals   | nonRepresentativeIntervals | intervals
        []                        | []                         | "both empty intervals"
        ['2018-01/2018-02']       | []                         | "empty interval in rep. availability"
        []                        | ['2018-01/2018-02']        | "empty interval in non-rep. availability"
        ['2017-01/2017-02']       | ['2017-01/2017-02']        | "full overlap (start/end, start/end)"
        ['2017-01/2017-02']       | ['2018-01/2018-02']        | "0 overlap (-10/-1, 0/10)"
        ['2017-01/2017-02']       | ['2017-02/2017-03']        | "0 overlap abutting (-10/0, 0/10)"
        ['2017-01/2017-02']       | ['2017-01/2017-03']        | "partial front overlap (0/10, 5/15)"
        ['2017-01/2017-03']       | ['2016-10/2017-02']        | "partial back overlap (0/10, -5/5)"
        ['2017-01/2017-03']       | ['2017-03/2017-03']        | "full front overlap (0/10, 5/10)"
        ['2017-01/2017-02']       | ['2017-01/2017-01']        | "full back overlap (0/10, 0/5)"
        ['2017-01/2017-05']       | ['2017-02/2017-03']        | "fully contain (0/10, 3/9)"
    }

    @Unroll
    def "Unconstrained availability always reflects the unconstrained representative availability"() {
        setup: "representative availability reflects a set of available intervals"
        representativeAvailability.getAvailableIntervals() >> SimplifiedIntervalList.simplifyIntervals(
                parseIntervals(representativeIntervals)
        )

        and: "non-representative availability reflects another set of available intervals"
        nonRepresentativeAvailability.getAvailableIntervals() >> SimplifiedIntervalList.simplifyIntervals(
                parseIntervals(nonRepresentativeIntervals)
        )

        expect: "pure left union availability always reflects the availabilities from the representative availability"
        leftPureUnionAvailability.getAvailableIntervals() == SimplifiedIntervalList.simplifyIntervals(
                parseIntervals(representativeIntervals)
        )

        where:
        representativeIntervals   | nonRepresentativeIntervals | intervals
        []                        | []                         | "both empty intervals"
        ['2018-01/2018-02']       | []                         | "empty interval in rep. availability"
        []                        | ['2018-01/2018-02']        | "empty interval in non-rep. availability"
        ['2017-01/2017-02']       | ['2017-01/2017-02']        | "full overlap (start/end, start/end)"
        ['2017-01/2017-02']       | ['2018-01/2018-02']        | "0 overlap (-10/-1, 0/10)"
        ['2017-01/2017-02']       | ['2017-02/2017-03']        | "0 overlap abutting (-10/0, 0/10)"
        ['2017-01/2017-02']       | ['2017-01/2017-03']        | "partial front overlap (0/10, 5/15)"
        ['2017-01/2017-03']       | ['2016-10/2017-02']        | "partial back overlap (0/10, -5/5)"
        ['2017-01/2017-03']       | ['2017-03/2017-03']        | "full front overlap (0/10, 5/10)"
        ['2017-01/2017-02']       | ['2017-01/2017-01']        | "full back overlap (0/10, 0/5)"
        ['2017-01/2017-05']       | ['2017-02/2017-03']        | "fully contain (0/10, 3/9)"
    }

    @Unroll
    def "Constrained availability always reflects the Constrained representative availability"() {
        given: "a constraint"
        PhysicalDataSourceConstraint constraint = Mock(PhysicalDataSourceConstraint)

        and: "representative availability reflects a set of constrained intervals"
        representativeAvailability.getAvailableIntervals(constraint) >> SimplifiedIntervalList.simplifyIntervals(
                parseIntervals(representativeIntervals)
        )

        and: "non-representative availability reflects another set of constrained intervals"
        nonRepresentativeAvailability.getAvailableIntervals(constraint) >> SimplifiedIntervalList.simplifyIntervals(
                parseIntervals(nonRepresentativeIntervals)
        )

        expect: "pure left union availability always reflects the availabilities from the representative availability"
        leftPureUnionAvailability.getAvailableIntervals(constraint) == SimplifiedIntervalList.simplifyIntervals(
                parseIntervals(representativeIntervals)
        )

        where:
        representativeIntervals   | nonRepresentativeIntervals | intervals
        []                        | []                         | "both empty intervals"
        ['2018-01/2018-02']       | []                         | "empty interval in rep. availability"
        []                        | ['2018-01/2018-02']        | "empty interval in non-rep. availability"
        ['2017-01/2017-02']       | ['2017-01/2017-02']        | "full overlap (start/end, start/end)"
        ['2017-01/2017-02']       | ['2018-01/2018-02']        | "0 overlap (-10/-1, 0/10)"
        ['2017-01/2017-02']       | ['2017-02/2017-03']        | "0 overlap abutting (-10/0, 0/10)"
        ['2017-01/2017-02']       | ['2017-01/2017-03']        | "partial front overlap (0/10, 5/15)"
        ['2017-01/2017-03']       | ['2016-10/2017-02']        | "partial back overlap (0/10, -5/5)"
        ['2017-01/2017-03']       | ['2017-03/2017-03']        | "full front overlap (0/10, 5/10)"
        ['2017-01/2017-02']       | ['2017-01/2017-01']        | "full back overlap (0/10, 0/5)"
        ['2017-01/2017-05']       | ['2017-02/2017-03']        | "fully contain (0/10, 3/9)"
    }

    def "toString matches Javadoc description"() {
        given: "toString spec of representative availability"
        representativeAvailability.toString() >> "representativeAvailability"
        nonRepresentativeAvailability.toString() >> "nonRepresentativeAvailability"

        expect: "Availability prints to have the same format described in toString() Javadoc of this Availability"
        leftPureUnionAvailability.toString() ==
                """
                LeftPureUnionAvailability{
                        allAvailabilities=[representativeAvailability, nonRepresentativeAvailability],
                        dataSources=[source1, source2],
                        representativeAvailability=representativeAvailability
                }
                """.replaceAll( /\n\s*/, "" ).replace("],", "], ")
    }

    def "LeftPureUnionAvailability explicitly implements getAvailableIntervals and doesn't implement the PhysicalDataSourceConstraint signature removed from the interface"() {
        setup:
        Availability mockAvailability = Mock(Availability)
        Availability irrelevant = Mock(Availability)
        irrelevant.getDataSourceNames() >> (["irrelevant"] as Set)
        mockAvailability.getDataSourceNames() >> (["mock"] as Set)

        LeftPureUnionAvailability leftPureUnionAvailability = new LeftPureUnionAvailability(
                mockAvailability,
                [irrelevant] as Set
        )
        SimplifiedIntervalList simplifiedIntervalList = new SimplifiedIntervalList()
        PhysicalDataSourceConstraint physicalDataSourceConstraint = Mock(PhysicalDataSourceConstraint)

        when:
        leftPureUnionAvailability.getAvailableIntervals((DataSourceConstraint) physicalDataSourceConstraint)

        and: "PhysicalDataSourceConstraint formerly had it's own method signature which might still be implemented in subclasses"
        leftPureUnionAvailability.getAvailableIntervals(physicalDataSourceConstraint)

        then: "Calls hit the method signature from the interface"
        2 * mockAvailability.getAvailableIntervals(_ as DataSourceConstraint) >> simplifiedIntervalList

        and: "Both paths hit explicit methods, not interface default implementation"
        0 * mockAvailability.getAvailableIntervals()
    }
}
