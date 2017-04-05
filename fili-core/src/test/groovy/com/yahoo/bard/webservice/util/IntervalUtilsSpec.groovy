// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.MONTH
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.WEEK
import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.YEAR

import com.yahoo.bard.webservice.data.time.ZonedTimeGrain
import com.yahoo.bard.webservice.druid.model.query.AllGranularity
import com.yahoo.bard.webservice.druid.model.query.Granularity

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Duration
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

class IntervalUtilsSpec extends Specification {

    def setupSpec() {
        DateTimeZone.setDefault(DateTimeZone.UTC)
    }

    SimplifiedIntervalList buildIntervalList(Collection<String> intervals) {
        intervals.collect { new Interval(it) } as SimplifiedIntervalList
    }

    Map buildIntervalMap(Collection<String> intervals) {
        int i = 0;
        intervals.collectEntries { [(new Interval(it)): i++] }
    }

    // 2013 is the most recent common (non leap) year that started on a Monday. This date is aligned with every period.
    static final DateTime EPOCH = new DateTime(2013, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);

    def "Validate #intervalName overlaps with #intervalsToMatch"() {
        given:
        Interval interval = new Interval(intervalName)
        List<Interval> match = buildIntervalList(intervalsToMatch)
        List<Interval> expected = buildIntervalList(expectedMatch)

        expect:
        IntervalUtils.getIntervalOverlaps(interval, match).collect(Collectors.toList()) == expected

        where:
        intervalName | intervalsToMatch                                   || expectedMatch
        "2013/2016"  | ["2014/2015"]                                      || ["2014/2015"]
        "2013/2016"  | ["2014-01-01/2014-02-01", "2014-02-01/2014-03-01"] || ["2014-01-01/2014-02-01", "2014-02-01/2014-03-01"]
        "2013/2014"  | ["2010/2020"]                                      || ["2013/2014"]
        "2013/2015"  | ["2014/2016"]                                      || ["2014/2015"]
        "2013/2015"  | ["2014/2016"]                                      || ["2014/2015"]
        "2013/2014"  | ["2014/2016"]                                      || []
        "2013/2014"  | ["2013-03-01/2014-04-01", "2014/2016"]             || ["2013-03-01/2014-01-01"]
    }

    @Unroll
    def "getOverlappingIntervals returns #expectedMatch with intervals #listOne and #listTwo"() {
        given:
        List<Interval> list1 = buildIntervalList(listOne)
        List<Interval> list2 = buildIntervalList(listTwo)
        Set<Interval> expected = buildIntervalList(expectedMatch) as Set

        expect:
        IntervalUtils.getOverlappingSubintervals(list1, list2) == expected
        IntervalUtils.getOverlappingSubintervals(list2, list1) == expected

        where:
        listOne                    | listTwo                                            || expectedMatch
        ["2013/2016"]              | ["2014/2015"]                                      || ["2014/2015"]
        ["2013/2016"]              | ["2014-01-01/2014-02-01", "2014-02-01/2014-03-01"] || ["2014-01-01/2014-02-01", "2014-02-01/2014-03-01"]
        ["2013/2014"]              | ["2010/2020"]                                      || ["2013/2014"]
        ["2013/2015"]              | ["2014/2016"]                                      || ["2014/2015"]
        ["2013/2015"]              | ["2014/2016"]                                      || ["2014/2015"]
        ["2013/2014"]              | ["2014/2016"]                                      || []
        ["2013/2014"]              | ["2013-03-01/2014-04-01", "2014/2016"]             || ["2013-03-01/2014-01-01"]
        ["2013/2015", "2016/2018"] | ["2014/2016", "2015/2017"]                         || ["2014/2015", "2016/2017"]
        ["2013/2015"]              | ["2013-03-01/2014-04-01", "2014/2016"]             || ["2014/2015", "2013-03-01/2014-04-01"]
    }

    @Unroll
    def "Slicing Intervals #baseIntervals with period #grain returns #expected"(
            Granularity grain,
            List<String> baseIntervals,
            List<String> expected
    ) {
        given:
        List<Interval> baseIntervalList = buildIntervalList(baseIntervals)
        Map<Interval, AtomicInteger> expectedSet = buildIntervalMap(expected)

        expect:
        IntervalUtils.getSlicedIntervals(baseIntervalList, grain).
                collectEntries { k, v -> [(k): v.get()] } == expectedSet

        where:
        [grain, baseIntervals, expected] << sliceExpectedSets()
    }

    static def sliceExpectedSets() {
        List results = []
        results.add(
                [DAY, ["2015-01-14/2015-01-17"], ["2015-01-14/2015-01-15", "2015-01-15/2015-01-16", "2015-01-16/2015-01-17"]]

        )
        results.add(
                [MONTH, ["2015-01/2015-03"], ["2015-01/2015-02","2015-02/2015-03"]]

        )
        results.add(
                [MONTH, ["2015-01/2015-03", "2015-02/2015-03"], ["2015-01/2015-02","2015-02/2015-03"]]
        )

        results.add(
                [new AllGranularity(), ["2015-01-14/2015-02-15", "2015-03-15/2015-03-16", "2015-04-16/2015-05-17"],
                 ["2015-01-14/2015-02-15", "2015-03-15/2015-03-16", "2015-04-16/2015-05-17"]]
        )

        // Single Interval
        results.add(
                [DAY, ["2015-01-14/2015-01-15"], ["2015-01-14/2015-01-15"] ]

        )
        // Empty Interval
        results.add(
                [DAY, [], []]

        )

        // Alignment is not guaranteed by the iterator
        results.add(
                [DAY, ["2015-01-14T10:00:00.000Z/2015-01-16T10:00:00.000Z"],
                 ["2015-01-14T10:00:00.000Z/2015-01-15T10:00:00.000Z", "2015-01-15T10:00:00.000Z/2015-01-16T10:00:00.000Z"]]
        )
        results.add(
                [DAY, ["2015-01-14/2015-01-16T10:00:00.000Z"],
                 ["2015-01-14/2015-01-15",
                  "2015-01-15/2015-01-16",
                  "2015-01-16/2015-01-16T10:00:00.000Z",
                 ]]
        )
        results.add(
                [WEEK, ["2014-06-30/2014-07-21"],
                 ["2014-06-30/2014-07-07",
                  "2014-07-07/2014-07-14",
                  "2014-07-14/2014-07-21"
                 ]]
        )
        // Month wrapping is done correctly even in the ragged end of months
        results.add(
                [MONTH, ["2014-01-30/2014-04-30"],
                 ["2014-01-30/2014-02-28", "2014-02-28/2014-03-30", "2014-03-30/2014-04-30"]]
        )

        return results
    }

    @Unroll
    def "Test that complement cuts out the correct hole(s) #comment"() {
        given:
        SimplifiedIntervalList from = buildIntervalList(fromAsStrings)
        SimplifiedIntervalList remove = buildIntervalList(removeAsStrings)
        SimplifiedIntervalList expected = buildIntervalList(expectedAsStrings)

        expect:
        IntervalUtils.collectBucketedIntervalsNotInIntervalList(remove, from, granularity) == expected

        where:
        [granularity, comment, fromAsStrings, removeAsStrings, expectedAsStrings] << complementExpectedSets()
    }

    static def complementExpectedSets() {
        List results = []
        results.add( [MONTH, "No source interval",
                      [],
                      ["2015-01-01/2017-01-01"],
                      [] ]
        )
        results.add( [MONTH, "No remove intervals",
                      ["2015-01-01/2017-01-01"],
                      [],
                      ["2015-01-01/2017-01-01"] ]
        )
        results.add( [MONTH, "No overlap",
                      ["2015-01-01/2017-01-01"],
                      ["2010/2011"],
                      ["2015-01-01/2017-01-01"]]
        )
        results.add( [MONTH, "Cut a hole out of the middle",
                      ["2015-01-01/2017-01-01"],
                      ["2016-01-01/2016-02-01"],
                      ["2015-01-01/2016-01-01", "2016-02-01/2017-01-01"] ]
        )
        results.add( [MONTH, "Shave off both ends",
                      ["2015/2017"],
                      ["2014-01-01/2015-02-01", "2016-05-01/2018"],
                      ["2015-02-01/2016-05-01"]
        ]
        )
        results.add( [YEAR, "Normalizes correctly",
                      ["2015/2017", "2016/2020"],
                      ["2016/2018", "2017/2019"],
                      ["2015/2016","2019/2020"]
        ]
        )
        results.add( [YEAR, "A single hole of smaller grain",
                      ["2015/2018"],
                      ["2016-01-01/2016-02-01"],
                      ["2015/2018"]
        ]
        )
        results.add( [YEAR, "A single hole of larger than grain but not aligned",
                      ["2015/2018"],
                      ["2015-12-01/2017-02-01"],
                      ["2015/2016", "2017/2018"]
        ]
        )
        results.add( [YEAR, "A hole larger than the request",
                      ["2015/2018"],
                      ["2014/2020"],
                      [] ]
        )
        return results
    }

    @Unroll
    def "Intersection of #supply yields #expected with fixed request and grain"() {
        setup:
        SimplifiedIntervalList requestedIntervals = buildIntervalList(['2014/2015'])
        Granularity granularity = MONTH
        SimplifiedIntervalList supplyIntervals = buildIntervalList(supply)
        SimplifiedIntervalList expectedIntervals = buildIntervalList(expected)

        expect:
        IntervalUtils.collectBucketedIntervalsIntersectingIntervalList(
                supplyIntervals,
                requestedIntervals,
                granularity
        ) == expectedIntervals

        where:
        supply                  | expected
        ["2013-04/2014-04"]     | ["2014-01/2014-04"]
        ["2014-04/2014-05"]     | ["2014-04/2014-05"]
        ["2014-09/2016-05"]     | ["2014-09/2015"]
        ["2012-09/2016-05"]     | ["2014/2015"]
        ["2012/2013"]           | []
        ["2020/2023"]           | []
    }

    @Unroll
    def "Intersect of #requestedIntervals by #grain yields #expected intersected with fixed supply"() {
        setup:
        SimplifiedIntervalList supply = buildIntervalList(['2012-05-04/2017-02-03'])
        Granularity granularity = grain
        SimplifiedIntervalList requestedIntervals = buildIntervalList(requested)
        SimplifiedIntervalList expectedIntervals = buildIntervalList(expected)

        expect:
        IntervalUtils.collectBucketedIntervalsIntersectingIntervalList(
                supply,
                requestedIntervals,
                granularity
        ) == expectedIntervals

        where:
        grain | requested              | expected
        YEAR  | ["2012/2017"]          | ["2012/2017"]
        MONTH | ["2012-02/2017"]       | ["2012-05/2017"]
        DAY   | ["2012-02-02/2016-05"] | ["2012-05-04/2016-05"]
        YEAR  | ["2013/2018"]          | ["2013/2018"]
        MONTH | ["2013/2017-03"]       | ["2013/2017-03"]
        DAY   | ["2014-02-02/2017-01"] | ["2014-02-02/2017-01"]
    }

    @Unroll
    def "Complement of #supply yields #expected with fixed request and grain"() {
        setup:
        SimplifiedIntervalList supplyIntervals = buildIntervalList(supply)
        SimplifiedIntervalList requestedIntervals = buildIntervalList(['2014/2015'])
        Granularity granularity = MONTH
        SimplifiedIntervalList expectedIntervals = buildIntervalList(expected)

        expect:
        IntervalUtils.collectBucketedIntervalsNotInIntervalList(
                supplyIntervals,
                requestedIntervals,
                granularity
        ) == expectedIntervals

        where:
        supply               | expected
        ["2013-04/2014-04"]  | ["2014-04/2015"]
        ["2014-04/2014-05"]  | ["2014/2014-04", "2014-05/2015"]
        ["2012-09/2016-05"]  | []
        ["2012/2013"]        | ["2014/2015"]
        ["2020/2023"]        | ["2014/2015"]
    }

    @Unroll
    def "Collect of #requestedIntervals by #grain yields #expected when fixed supply is removed"() {
        setup:
        SimplifiedIntervalList supply = buildIntervalList(['2012-05-04/2017-02-03'])
        SimplifiedIntervalList expectedIntervals = buildIntervalList(expected)
        SimplifiedIntervalList requestedIntervals = buildIntervalList(requested)
        Granularity granularity = grain

        expect:
        IntervalUtils.collectBucketedIntervalsNotInIntervalList(
                supply,
                requestedIntervals,
                granularity
        ) == expectedIntervals

        where:
        grain | requested                 | expected
        YEAR  | ["2012/2017"]             | ["2012/2013"]
        MONTH | ["2012-02/2017"]          | ["2012-02/2012-06"]
        DAY   | ["2012-02-02/2016-05"]    | ["2012-02-02/2012-05-04"]
        DAY   | ["2012-06/2016-05"]       | []
        YEAR  | ["2013/2018"]             | ["2017/2018"]
        MONTH | ["2013/2017-04"]          | ["2017-02/2017-04"]
        DAY   | ["2012-02-03/2017-04-04"] | ["2012-02-03/2012-05-04", "2017-02-03/2017-04-04"]
    }

    @Unroll
    def "getCoarsestTimeGrain returns duration of #expected among #duration1, #duration2, #duration3"() {
        given:
        ZonedTimeGrain zonedTimeGrain1 = Mock(ZonedTimeGrain)
        ZonedTimeGrain zonedTimeGrain2 = Mock(ZonedTimeGrain)
        ZonedTimeGrain zonedTimeGrain3 = Mock(ZonedTimeGrain)

        zonedTimeGrain1.getEstimatedDuration() >> new Duration(duration1)
        zonedTimeGrain2.getEstimatedDuration() >> new Duration(duration2)
        zonedTimeGrain3.getEstimatedDuration() >> new Duration(duration3)

        expect:
        IntervalUtils.getCoarsestTimeGrain([zonedTimeGrain1, zonedTimeGrain2, zonedTimeGrain3]).getEstimatedDuration().equals(new Duration(expected))

        where:
        duration1 | duration2 | duration3 | expected
        1         | 2         | 3         | 1
        1         | 1         | 3         | 1
        3         | 3         | 3         | 3
    }

    def "getCoarsestTimeGrain returns null on empty input time grain collections"() {
        expect:
        IntervalUtils.getCoarsestTimeGrain(Collections.emptyList()) == null
    }

    /**
     * Returns the instant at which this year began.
     *
     * @return  The instant at which this year began.
     */
    def getStartThisYear() {
        new DateTime(DateTime.now().getYear(), 1, 1, 0, 0, 0, 0)
    }
}
