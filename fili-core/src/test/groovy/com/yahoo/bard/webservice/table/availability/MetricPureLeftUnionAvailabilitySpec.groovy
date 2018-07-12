// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.table.ConfigPhysicalTable
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import spock.lang.Specification
import spock.lang.Unroll

class MetricPureLeftUnionAvailabilitySpec extends Specification {
    Availability representativeAvailability
    Availability nonRepresentativeAvailability

    String metric

    Set<ConfigPhysicalTable> physicalTables

    MetricPureLeftUnionAvailability metricPureLeftUnionAvailability

    Map<Availability, Set<String>> availabilitiesToMetricNames

    def setup() {
        representativeAvailability = Mock(Availability)
        nonRepresentativeAvailability = Mock(Availability)

        representativeAvailability.dataSourceNames >> ([DataSourceName.of('source1')] as Set)
        nonRepresentativeAvailability.dataSourceNames >> ([DataSourceName.of('source2')] as Set)

        metric = 'metric'

        physicalTables = [
                Mock(ConfigPhysicalTable) {getAvailability() >> representativeAvailability},
                Mock(ConfigPhysicalTable) {getAvailability() >> nonRepresentativeAvailability}
        ] as Set

        availabilitiesToMetricNames = [
                (representativeAvailability): [metric] as Set,
                (nonRepresentativeAvailability): [metric] as Set
        ]

        metricPureLeftUnionAvailability = new MetricPureLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )
    }

    def "Build method and constructor produce the same instance"() {
        given: "a MetricPureLeftUnionAvailability instance crated by constructor"
        MetricPureLeftUnionAvailability availabilityByContr = new MetricPureLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        and: "a MetricPureLeftUnionAvailability instance created by builder with same parameter"
        MetricPureLeftUnionAvailability availabilityByBuild = MetricPureLeftUnionAvailability.build(
                [representativeAvailability] as Set,
                physicalTables,
                availabilitiesToMetricNames
        )

        expect: "the two instances are equal"
        availabilityByBuild == availabilityByContr
    }

    def "Without constraint, Availability returns immutable datasources of all participating Availabilities"() {
        when: "datasources are requested"
        Set<DataSourceName> dataSourceNames = metricPureLeftUnionAvailability.getDataSourceNames()

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
        Set<DataSourceName> dataSourceNames = metricPureLeftUnionAvailability.getDataSourceNames(constraint)

        then: "datasources are only associated with the representative availability"
        dataSourceNames.collect {it -> it.asName()} == ["constrainedSource1", "constrainedSource2"]

        when: "when we try to mutate the datasources"
        dataSourceNames.add(DataSourceName.of("hack"))

        then: "error is thrown"
        Exception exception = thrown()
        exception instanceof UnsupportedOperationException
    }

    @Unroll
    def "Availabilities by columns reflects representative availabilities only when rep. and non-rep. availabilities have #intervals"() {
        setup: "representative availabilities reflects a set of available intervals"
        representativeAvailability.getAllAvailableIntervals() >> [
                (metric): SimplifiedIntervalList.simplifyIntervals(
                        AvailabilityTestingUtils.parseIntervals(representativeIntervals)
                )
        ]

        and: "non-representative availabilities reflects another set of available intervals"
        nonRepresentativeAvailability.getAllAvailableIntervals() >> [
                (metric): SimplifiedIntervalList.simplifyIntervals(
                        AvailabilityTestingUtils.parseIntervals(nonRepresentativeIntervals)
                )
        ]

        expect: "pure left union availability always reflects the availabilities from the representative availabilities"
        metricPureLeftUnionAvailability.getAllAvailableIntervals() == [
                (metric): SimplifiedIntervalList.simplifyIntervals(
                        AvailabilityTestingUtils.parseIntervals(representativeIntervals)
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
    def "Unconstrained availability always reflects the unconstrained representative availabilities"() {
        setup: "representative availabilities reflects a set of available intervals"
        representativeAvailability.getAvailableIntervals() >> SimplifiedIntervalList.simplifyIntervals(
                AvailabilityTestingUtils.parseIntervals(representativeIntervals)
        )

        and: "non-representative availabilities reflects another set of available intervals"
        nonRepresentativeAvailability.getAvailableIntervals() >> SimplifiedIntervalList.simplifyIntervals(
                AvailabilityTestingUtils.parseIntervals(nonRepresentativeIntervals)
        )

        expect: "pure left union availability always reflects the availabilities from the representative availabilities"
        metricPureLeftUnionAvailability.getAvailableIntervals() == SimplifiedIntervalList.simplifyIntervals(
                AvailabilityTestingUtils.parseIntervals(representativeIntervals)
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
    def "Constrained availability always reflects the Constrained representative availabilities"() {
        given: "a constraint"
        PhysicalDataSourceConstraint constraint = Mock(PhysicalDataSourceConstraint)

        and: "representative availabilities reflects a set of constrained intervals"
        representativeAvailability.getAvailableIntervals(constraint) >> SimplifiedIntervalList.simplifyIntervals(
                AvailabilityTestingUtils.parseIntervals(representativeIntervals)
        )

        and: "non-representative availabilities reflects another set of constrained intervals"
        nonRepresentativeAvailability.getAvailableIntervals(constraint) >> SimplifiedIntervalList.simplifyIntervals(
                AvailabilityTestingUtils.parseIntervals(nonRepresentativeIntervals)
        )

        expect: "pure left union availability always reflects the availabilities from the representative availabilities"
        metricPureLeftUnionAvailability.getAvailableIntervals(constraint) == SimplifiedIntervalList.simplifyIntervals(
                AvailabilityTestingUtils.parseIntervals(representativeIntervals)
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

    def "Published reference to internal state is immutable"() {
        given: "a reference to representative availabilities"
        Set<Availability> availabilities = metricPureLeftUnionAvailability.getRepresentativeAvailabilities()

        when: "we try add a new element to the set"
        availabilities.add(Mock(Availability))

        then: "error is thworn"
        Exception exception = thrown()
        exception instanceof UnsupportedOperationException

        when: "we try to mutate an element to the set"
        Availability availability = availabilities.iterator().next()
        availability = null

        then: "internal state is not changed"
        availabilities == metricPureLeftUnionAvailability.representativeAvailabilities
    }

    def "toString matches Javadoc description"() {
        given: "toString spec of representative availability"
        representativeAvailability.toString() >> "representativeAvailability"
        nonRepresentativeAvailability.toString() >> "nonRepresentativeAvailability"

        expect: "Availability prints to have the same format described in toString() Javadoc of this Availability"
        metricPureLeftUnionAvailability.toString() ==
                """
                MetricPureLeftUnionAvailability{
                        allAvailabilities=[representativeAvailability, nonRepresentativeAvailability], 
                        dataSources=[source1, source2], 
                        representativeAvailabilities=[representativeAvailability]
                }
                """.replaceAll( /\n\s*/, "" );
    }

    @Unroll
    def "'#columnSet1' and '#columnSet2' trigger error because of #reason"() {
        given: "toString spec of representative and non-representative availabilities"
        representativeAvailability.toString() >> "rep."
        nonRepresentativeAvailability.toString() >> "non-rep."

        and: "expected error message"
        String errorMessage = reason == "empty column set(s)" ? "Empty column set found - '{rep.=${columnSet1}, non-rep.=${columnSet2}}'"
                : """Columns from multiple sources do not match - '{rep.=${columnSet1}, non-rep.=${columnSet2}}'"""

        and: "bad column sets are used"
        availabilitiesToMetricNames = [
                (representativeAvailability): columnSet1,
                (nonRepresentativeAvailability): columnSet2
        ]

        when: "the bad columns are validated"
        MetricPureLeftUnionAvailability.validateColumns(availabilitiesToMetricNames)

        then: "error is thrown"
        Exception exception = thrown()
        exception instanceof IllegalArgumentException
        exception.message == errorMessage

        when: "the bad columns are passed to constructor"
        new MetricPureLeftUnionAvailability(
                [representativeAvailability] as Set,
                physicalTables.availability as Set,
                availabilitiesToMetricNames
        )

        then: "error is also thrown"
        exception = thrown()
        exception instanceof IllegalArgumentException
        exception.message == errorMessage

        where:
        columnSet1              | columnSet2              | reason
        [] as Set               | [] as Set               | "empty column set(s)"
        ["col1"] as Set         | [] as Set               | "empty column set(s)"
        [] as Set               | ["col2"] as Set         | "empty column set(s)"
        ["col1", "col2"] as Set | ["col1"] as Set         | "duplicate column sets"
        ["col1"] as Set         | ["col1", "col2"] as Set | "duplicate column sets"
        ["col1", "col2"] as Set | ["col3"] as Set         | "duplicate column sets"
        ["col3"] as Set         | ["col1", "col2"] as Set | "duplicate column sets"
    }

    @Unroll
    def "'#columnSet1' and '#columnSet2' do not trigger error"() {
        when: "valid column sets are used"
        MetricPureLeftUnionAvailability.validateColumns(
                [
                        (representativeAvailability): columnSet1,
                        (nonRepresentativeAvailability): columnSet2
                ]
        )

        then: "no error is thrown"
        noExceptionThrown()

        where:
        columnSet1              | columnSet2
        ["col1"] as Set         | ["col1"] as Set
        ["col1", "col2"] as Set | ["col1", "col2"] as Set
    }

    @Unroll
    def "There #is empty column set(s) among '#columnSet1' and '#columnSet2'"() {
        expect:
        MetricPureLeftUnionAvailability.hasEmptyColSet(
                [
                        (representativeAvailability): columnSet1,
                        (nonRepresentativeAvailability): columnSet2
                ]
        ) == hasEmpty

        where:
        columnSet1      | columnSet2      || hasEmpty
        [] as Set       | [] as Set       || Boolean.TRUE
        ["col1"] as Set | [] as Set       || Boolean.TRUE
        [] as Set       | ["col2"] as Set || Boolean.TRUE
        ["col1"] as Set | ["col1"] as Set || Boolean.FALSE

        is = hasEmpty ? "is(are)" : "is(are) not"
    }

    @Unroll
    def "'#columnSet1' and '#columnSet2' #are the same column set"() {
        expect:
        MetricPureLeftUnionAvailability.hasDifferentColSet(
                [
                        (representativeAvailability): columnSet1,
                        (nonRepresentativeAvailability): columnSet2
                ]
        ) == hasDifferentColSet

        where:
        columnSet1              | columnSet2              || hasDifferentColSet
        [] as Set               | [] as Set               || Boolean.FALSE
        ["col1"] as Set         | [] as Set               || Boolean.TRUE
        [] as Set               | ["col2"] as Set         || Boolean.TRUE
        ["col1"] as Set         | ["col1"] as Set         || Boolean.FALSE
        ["col1", "col2"] as Set | ["col1"] as Set         || Boolean.TRUE
        ["col1"] as Set         | ["col1", "col2"] as Set || Boolean.TRUE
        ["col1", "col2"] as Set | ["col3"] as Set         || Boolean.TRUE
        ["col3"] as Set         | ["col1", "col2"] as Set || Boolean.TRUE
        ["col1", "col2"] as Set | ["col1", "col2"] as Set || Boolean.FALSE

        are = hasDifferentColSet ? "are not" : "are"
    }
}
