// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class IntervalEndComparatorSpec extends Specification {

    @Unroll
    def "#intervalA is #comparisonDescription #intervalB"() {
        given: "An IntervalEndComparator"
        IntervalEndComparator comparator = new IntervalEndComparator()

        expect: "Intervals are correctly compared"
        comparator.compare(new Interval(intervalA), new Interval(intervalB)) == comparisonValue

        where:
        intervalA               | intervalB               | comparisonValue | comparisonDescription
        "2014-01-01/2014-01-05" | "2014-01-07/2014-01-10" | -1              | "less than"
        "2014-01-07/2014-01-10" | "2014-01-01/2014-01-05" | 1               | "greater than"
        "2014-01-01/2014-01-05" | "2014-01-01/2014-01-05" | 0               | "equal to"
    }

    def "A TreeSet of Intervals is sorted in ascending order when using an IntervalEndComparator"() {
        given: "A TreeSet using an IntervalEndComparator"
        TreeSet<Interval> treeSet = new TreeSet<>(new IntervalEndComparator())

        and: "A list of intervals in the expected (sorted) order"
        List<Interval> intervalSortedList = [
                "2014-01-01/2014-01-05",
                "2014-01-07/2014-01-15",
                "2014-01-18/2014-01-25"
        ].collect {new Interval(it)}

        when: "We add the intervals to the TreeSet in a different order than they should be"
        treeSet.add(intervalSortedList[2])
        treeSet.add(intervalSortedList[0])
        treeSet.add(intervalSortedList[1])

        then: "The TreeSet gives them back in the expected (sorted) order"
        treeSet.asList() == intervalSortedList
    }
}
