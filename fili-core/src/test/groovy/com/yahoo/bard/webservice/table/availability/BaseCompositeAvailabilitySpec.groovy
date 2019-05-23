// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability

import com.yahoo.bard.webservice.data.config.names.DataSourceName
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint
import com.yahoo.bard.webservice.util.SimplifiedIntervalList

import org.joda.time.DateTime

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.Stream

class BaseCompositeAvailabilitySpec extends Specification {

    /**
     * Simple class extending BaseCompositeAvailability to allow for testing of its methods
     */
    class SimpleCompositeAvailability extends BaseCompositeAvailability {

        /**
         * Constructor.
         *
         * @param availabilityStream A potentially ordered stream of availabilities which supply this composite view
         */
        protected SimpleCompositeAvailability(Stream<Availability> availabilityStream) {
            super(availabilityStream)
        }

        @Override
        SimplifiedIntervalList getAvailableIntervals(final DataSourceConstraint constraint) {
            return getAvailableIntervals()
        }
    }

    Availability availability1
    Availability availability2
    Availability availability3

    Optional<DateTime> start_1
    Optional<DateTime> end_1

    Optional<DateTime> start_2
    Optional<DateTime> end_2

    Optional<DateTime> start_3
    Optional<DateTime> end_3

    @Shared DateTime earliestStart =  new DateTime(1980, 1, 1, 0, 0)
    @Shared DateTime middleStart = new DateTime(1990, 1, 1, 0, 0)
    @Shared DateTime latestStart = new DateTime(2000, 1, 1, 0, 0)


    @Shared DateTime earliestEnd = new DateTime(2010, 1, 1, 0, 0)
    @Shared DateTime middleEnd = new DateTime(2020, 1, 1, 0, 0)
    @Shared DateTime latestEnd = new DateTime(2030, 1, 1, 0, 0)

    SimpleCompositeAvailability compositeAvailability

    def setup() {
        availability1 = Mock(Availability)
        availability2 = Mock(Availability)
        availability3 = Mock(Availability)

        start_1 = Optional.empty()
        start_2 = Optional.empty()
        start_3 = Optional.empty()

        end_1 = Optional.empty()
        end_2 = Optional.empty()
        end_3 = Optional.empty()

        availability1.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> { start_1 }
        availability1.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> { end_1 }

        availability2.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> { start_2 }
        availability2.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> { end_2 }

        availability3.getExpectedStartDate(_ as PhysicalDataSourceConstraint) >> { start_3 }
        availability3.getExpectedEndDate(_ as PhysicalDataSourceConstraint) >> { end_3 }

        availability1.getExpectedStartDate(_ as DataSourceConstraint) >> { start_1 }
        availability1.getExpectedEndDate(_ as DataSourceConstraint) >> { end_1 }

        availability2.getExpectedStartDate(_ as DataSourceConstraint) >> { start_2 }
        availability2.getExpectedEndDate(_ as DataSourceConstraint) >> { end_2 }

        availability3.getExpectedStartDate(_ as DataSourceConstraint) >> { start_3 }
        availability3.getExpectedEndDate(_ as DataSourceConstraint) >> { end_3 }

        availability1.getDataSourceNames() >> ([DataSourceName.of("dsName_1")] as Set<DataSourceName>)
        availability2.getDataSourceNames() >> ([DataSourceName.of("dsName_2")] as Set<DataSourceName>)
        availability3.getDataSourceNames() >> ([DataSourceName.of("dsName_3")] as Set<DataSourceName>)

        compositeAvailability = new SimpleCompositeAvailability(
                Stream.of(
                        availability1,
                        availability2,
                        availability3
                )
        )
    }

    @Unroll
    def "If #desc availabilities have no start date, the composite has no start date"() {
        given:
        start_1 = Optional.ofNullable((DateTime) testExpectedStart_1)
        start_2 = Optional.ofNullable((DateTime) testExpectedStart_2)
        start_3 = Optional.ofNullable((DateTime) testExpectedStart_3)

        Optional<DateTime> result_1 = compositeAvailability.getExpectedStartDate(Mock(PhysicalDataSourceConstraint))
        Optional<DateTime> result_2 = compositeAvailability.getExpectedStartDate(Mock(DataSourceConstraint))

        expect:
        !result_1.isPresent()
        !result_2.isPresent()

        where:
        testExpectedStart_1 |   testExpectedStart_2 |   testExpectedStart_3 |   desc
        earliestStart       |   middleStart         |   null                |   "one"
        earliestStart       |   null                |   null                |   "two"
        null                |   null                |   null                |   "all"
    }

    @Unroll
    def "If #desc availabilities have no end date, the composite has no end date"() {
        given:
        end_1 = Optional.ofNullable((DateTime) testExpectedEnd_1)
        end_2 = Optional.ofNullable((DateTime) testExpectedEnd_2)
        end_3 = Optional.ofNullable((DateTime) testExpectedEnd_3)

        Optional<DateTime> result_1  = compositeAvailability.getExpectedEndDate(Mock(PhysicalDataSourceConstraint))
        Optional<DateTime> result_2  = compositeAvailability.getExpectedEndDate(Mock(DataSourceConstraint))

        expect:
        !result_1.isPresent()
        !result_2.isPresent()

        where:
        testExpectedEnd_1   |   testExpectedEnd_2   |   testExpectedEnd_3   |   desc
        earliestEnd         |   middleEnd           |   null                |   "one"
        earliestEnd         |   null                |   null                |   "two"
        null                |   null                |   null                |   "all"
    }

    @Unroll
    def "earliest start is at position #pos in the availabilities stream"() {
        given:
        start_1 = Optional.ofNullable((DateTime) testExpectedStart_1)
        start_2 = Optional.ofNullable((DateTime) testExpectedStart_2)
        start_3 = Optional.ofNullable((DateTime) testExpectedStart_3)

        Optional<DateTime> result_1 = compositeAvailability.getExpectedStartDate(Mock(PhysicalDataSourceConstraint))
        Optional<DateTime> result_2 = compositeAvailability.getExpectedStartDate(Mock(DataSourceConstraint))

        expect:
        result_1.isPresent()
        result_1.get() == earliestStart

        result_2.isPresent()
        result_2.get() == earliestStart

        where:
        testExpectedStart_1 |   testExpectedStart_2 |   testExpectedStart_3 |   desc
        earliestStart       |   middleStart         |   latestStart         |   "first"
        latestStart         |   earliestStart       |   middleStart         |   "middle"
        middleStart         |   latestStart         |   earliestStart       |   "last"
    }

    @Unroll
    def "latest end is at position #desc in the availabilities stream"() {
        given:
        end_1 = Optional.ofNullable((DateTime) testExpectedEnd_1)
        end_2 = Optional.ofNullable((DateTime) testExpectedEnd_2)
        end_3 = Optional.ofNullable((DateTime) testExpectedEnd_3)

        Optional<DateTime> result_1 = compositeAvailability.getExpectedEndDate(Mock(PhysicalDataSourceConstraint))
        Optional<DateTime> result_2 = compositeAvailability.getExpectedEndDate(Mock(DataSourceConstraint))

        expect:
        result_1.isPresent()
        result_1.get() == latestEnd

        result_2.isPresent()
        result_2.get() == latestEnd

        where:
        testExpectedEnd_1   |   testExpectedEnd_2   |   testExpectedEnd_3   |   desc
        latestEnd           |   middleEnd           |   earliestEnd         |   "first"
        middleEnd           |   latestEnd           |   earliestEnd         |   "middle"
        earliestEnd         |   middleEnd           |   latestEnd           |   "last"
    }

    def "getting expected end and start dates work with 0 and 1 availabilities in collection"() {
        // start with 0 item
        setup:
        compositeAvailability = new SimpleCompositeAvailability(
                Stream.empty()
        )

        when:
        Optional<DateTime> startResult_1 = compositeAvailability.getExpectedStartDate(Mock(PhysicalDataSourceConstraint))
        Optional<DateTime> startResult_2 = compositeAvailability.getExpectedStartDate(Mock(DataSourceConstraint))
        Optional<DateTime> endResult_1 = compositeAvailability.getExpectedEndDate(Mock(PhysicalDataSourceConstraint))
        Optional<DateTime> endResult_2 = compositeAvailability.getExpectedEndDate(Mock(DataSourceConstraint))

        then:
        !startResult_1.isPresent()
        !startResult_2.isPresent()
        !endResult_1.isPresent()
        !endResult_2.isPresent()

        // now 1 item but it has empty start and end
        when:
        compositeAvailability = new SimpleCompositeAvailability(
                Stream.of(
                        availability1
                )
        )

        startResult_1 = compositeAvailability.getExpectedStartDate(Mock(PhysicalDataSourceConstraint))
        startResult_2 = compositeAvailability.getExpectedStartDate(Mock(DataSourceConstraint))
        endResult_1 = compositeAvailability.getExpectedEndDate(Mock(PhysicalDataSourceConstraint))
        endResult_2 = compositeAvailability.getExpectedEndDate(Mock(DataSourceConstraint))

        then:
        !startResult_1.isPresent()
        !startResult_2.isPresent()
        !endResult_1.isPresent()
        !endResult_2.isPresent()

        when:
        start_1 = Optional.of(earliestStart)
        end_1 = Optional.of(earliestEnd)

        startResult_1 = compositeAvailability.getExpectedStartDate(Mock(PhysicalDataSourceConstraint))
        startResult_2 = compositeAvailability.getExpectedStartDate(Mock(DataSourceConstraint))
        endResult_1 = compositeAvailability.getExpectedEndDate(Mock(PhysicalDataSourceConstraint))
        endResult_2 = compositeAvailability.getExpectedEndDate(Mock(DataSourceConstraint))

        then:
        startResult_1.isPresent()
        startResult_1.get() == earliestStart

        startResult_2.isPresent()
        startResult_2.get() == earliestStart

        endResult_1.isPresent()
        endResult_1.get() == earliestEnd

        endResult_2.isPresent()
        endResult_2.get() == earliestEnd
    }
}
