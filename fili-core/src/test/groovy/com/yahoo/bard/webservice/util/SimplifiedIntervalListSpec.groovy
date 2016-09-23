// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import org.joda.time.DateTime
import org.joda.time.Days
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll
/**
 *
 */
class SimplifiedIntervalListSpec extends Specification {

    static List<List<Integer>> tinyEvenIntervals
    static List<List<Integer>> tinyOddIntervals
    static List<List<Integer>> mediumIntervals
    static List<List<Integer>> largeEvenInterval

    def setupSpec() {
        tinyEvenIntervals = [[2, 4], [6, 10], [14, 30]]
        tinyOddIntervals = [[1, 3], [7, 9], [11, 15], [17, 19]]
        mediumIntervals = [[5, 11], [16, 21]]
        largeEvenInterval = [[4, 42]]
    }

    List buildIntervalList(Collection<String> intervals) {
        intervals.collect { new Interval(it) }
    }

    List buildIntervalListNum(List<List<Long>> times) {
        times.collect { new Interval(it[0], it[1]) }
    }

    @Unroll
    def "SimplfyIntervals returns #expected with input #input"() {
        given:
        List<Interval> raw = buildIntervalList(input)
        List<Interval> expectedIntervals = buildIntervalList(expected)

        expect:
        SimplifiedIntervalList.simplifyIntervals(raw) == expectedIntervals

        where:
        [input, expected] << simplifyExpectData()
    }


    @Unroll
    def "appendWithMerge produces #expected starting from #original with interval #addend"() {
        given:
        List<Interval> workingList = new SimplifiedIntervalList(buildIntervalList(original))
        List<Interval> expectedList = new SimplifiedIntervalList(buildIntervalList(expected))
        Interval interval = new Interval(addend)

        when:
        workingList.appendWithMerge(interval)

        then:
        workingList == expectedList

        where:
        [original, addend, expected] << appendWithMergeData()
    }

    def simplifyExpectData() {
        def results = []
        results << [['2014/2017', '2015/2020'],  ['2014/2020']]
        results << [['2014/2020'],  ['2014/2020']]
        results << [['2015/2016', '2013/2014'], ['2013/2014', '2015/2016']]
        results << [['2015/2015', '2015/2016', '2012/2013'],  ['2012/2013', '2015/2016']]
        results << [["2015-01/2015-02","2015-02/2015-03"], ["2015-01/2015-03"]]

        results <<
                [["2015-01-14T10:00:00.000Z/2015-01-15T10:00:00.000Z",
                  "2015-01-15T10:00:00.000Z/2015-01-16T10:00:00.000Z"],
                 ["2015-01-14T10:00:00.000Z/2015-01-16T10:00:00.000Z"]]
        results
    }

    def appendWithMergeData() {
        def results = []
        results << [["2015/2017"], "2018/2020", ["2015/2017", "2018/2020"]]
        results << [["2015/2018"], "2016/2020", ["2015/2020"]]
        results << [["2015/2016"], "2016/2020", ["2015/2020"]]
        results << [["2015/2016"], "2014/2017", ["2014/2017"]]
        results << [["2010/2011", "2014/2018"], "2015/2016", ["2010/2011", "2014/2018"]]
        results << [["2010/2011", "2014/2015", "2016/2017", "2018/2020", "2021/2022"], "2015/2021", ["2010/2011", "2014/2022"]]

        results << [["2010/2012", "2013/2014"], "2009/2012", ["2009/2012", "2013/2014"]]
        results << [["2010/2012", "2015/2016"], "2013/2014", ["2010/2012", "2013/2014", "2015/2016"]]
        results << [["2010/2012", "2015/2016", "2017/2018"], "2013/2014", ["2010/2012", "2013/2014", "2015/2016", "2017/2018"]]
        results << [["2010/2012", "2015/2016", "2017/2018"], "2012/2015", ["2010/2016", "2017/2018"]]

        results
    }

    @Unroll
    def "Union of #left and #right yields #expected"() {
        setup:
        SimplifiedIntervalList expectedList = buildIntervalListNum(expected)
        SimplifiedIntervalList leftList = buildIntervalListNum(left)
        SimplifiedIntervalList rightList = buildIntervalListNum(right)

        expect:
        leftList.union(rightList) == expectedList
        rightList.union(leftList) == expectedList

        where:
        left                | right             | expected
        tinyEvenIntervals   | tinyOddIntervals  | [[1, 4], [6, 10], [11, 30]]
        tinyEvenIntervals   | mediumIntervals   | [[2, 4], [5, 11], [14, 30]]
        tinyOddIntervals    | mediumIntervals   | [[1, 3], [5, 15], [16, 21]]
        tinyEvenIntervals   | tinyEvenIntervals | tinyEvenIntervals
        tinyEvenIntervals   | largeEvenInterval | [[2,42]]
        []                  | tinyEvenIntervals | tinyEvenIntervals
        []                  | []                | []
    }

    @Unroll
    def "Intersection of #left and #right yields #expected"() {
        setup:
        SimplifiedIntervalList expectedList = buildIntervalListNum(expected)
        SimplifiedIntervalList leftList = buildIntervalListNum(left)
        SimplifiedIntervalList rightList = buildIntervalListNum(right)

        expect:
        leftList.intersect(rightList) == expectedList
        rightList.intersect(leftList) == expectedList

        where:
        left                | right             | expected
        tinyEvenIntervals   | tinyOddIntervals  | [[2,3], [7,9], [14, 15], [17, 19]]
        tinyEvenIntervals   | mediumIntervals   | [[6, 10], [16, 21]]
        tinyOddIntervals    | mediumIntervals   | [[7, 9], [17, 19]]
        tinyEvenIntervals   | tinyEvenIntervals | tinyEvenIntervals
        tinyEvenIntervals   | largeEvenInterval | [[6,10], [14, 30]]
        []                  | tinyOddIntervals  | []
    }

    @Unroll
    def "Subtraction of #left and #right yields #expected"() {
        setup:
        SimplifiedIntervalList expectedList = buildIntervalListNum(expected)
        SimplifiedIntervalList leftList = buildIntervalListNum(left)
        SimplifiedIntervalList rightList = buildIntervalListNum(right)

        expect:
        leftList.subtract(rightList) == expectedList

        where:
        left                | right             | expected
        tinyEvenIntervals   | tinyOddIntervals  | [[3, 4], [6, 7], [9,10], [15, 17], [19, 30]]
        tinyOddIntervals    | tinyEvenIntervals | [[1, 2], [11, 14]]
        tinyEvenIntervals   | mediumIntervals   | [[2, 4], [14, 16], [21, 30]]
        tinyOddIntervals    | mediumIntervals   | [[1, 3], [11, 15]]
        tinyEvenIntervals   | tinyEvenIntervals | []
        tinyEvenIntervals   | largeEvenInterval | [[2, 4]]
        tinyEvenIntervals   | []                | tinyEvenIntervals
        []                  | tinyOddIntervals  | []
        []                  | []                | []
    }

    @Unroll
    def "Period Iterator creates period sliced starting at #expected when dividing #rawIntervals by #period"() {

        List<Interval> expectedIntervals = expected.collect() {
            new Interval(new DateTime(it), period)
        }
        Iterator<Interval> expectedIterator = expectedIntervals.iterator()
        List<Interval> raw = rawIntervals.collect() {
            new Interval(new DateTime(it[0]), it[1])
        }
        SimplifiedIntervalList intervals = new SimplifiedIntervalList(raw)
        when:
        Iterator<Interval> actual = intervals.periodIterator(period)

        then:
        while(expectedIterator.hasNext()) {
            assert expectedIterator.next() == actual.next()
        }
        ! actual.hasNext()

        where:
        period     | rawIntervals                               | expected
        Days.ONE   | [["2015", Days.THREE]]                     | ["2015-01-01", "2015-01-02", "2015-01-03"]
        Days.THREE | [["2015", Days.THREE]]                     | ["2015-01-01"]
        Days.ONE   | [["2015", Days.THREE], ["2013", Days.ONE]] | ["2013", "2015-01-01", "2015-01-02", "2015-01-03"]
        Days.THREE | [["2015", Days.THREE]]                     | ["2015-01-01"]
    }
}
