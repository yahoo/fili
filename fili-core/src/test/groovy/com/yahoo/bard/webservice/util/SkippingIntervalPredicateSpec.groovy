// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import com.yahoo.bard.webservice.util.SimplifiedIntervalList.IsSubinterval

import org.joda.time.DateTime
import org.joda.time.Interval

import spock.lang.Specification
import spock.lang.Unroll

class SkippingIntervalPredicateSpec extends Specification {

    SimplifiedIntervalList buildIntervalList(Collection<String> intervals) {
        intervals.collect { new Interval(it) } as SimplifiedIntervalList
    }

    @Unroll
    def "skipAhead #comment with #target "() {
        given:
        SimplifiedIntervalList.SkippingIntervalPredicate subinterval =
                new SimplifiedIntervalList.SkippingIntervalPredicate(
                        buildIntervalList(["2015/2017", "2018/2020"]),
                        SimplifiedIntervalList.IsSubinterval.IS_SUBINTERVAL,
                        false
                )
        DateTime skipTo = new DateTime(target)
        Interval expectedNext = expectedNextString == null ? null : new Interval(expectedNextString)

        when:
        subinterval.skipAhead(skipTo);

        then:
        subinterval.activeInterval == expectedNext
        subinterval.supply.hasNext() == expectedHasNext

        where:
        target       | expectedNextString  |  expectedHasNext  |  comment
        "2021-01-01" | null                |  false            |  "Date beyond end exhausts the iterator"
        "2011-01-01" | "2015/2017"         |  true             |  "Date before start doesnt change the iterator"
        "2015-01-01" | "2015/2017"         |  true             |  "Date doesn't advance on inclusive start"
        "2019-01-01" | "2018/2020"         |  false            |  "Date doesn't advance beyond interval on interior date"
        "2017-01-01" | "2018/2020"         |  false            |  "Date advances on exclusive end"
    }

    @Unroll
    def "Subinterval test returns value is #result for #testInterval"() {
        given:
        IsSubinterval subinterval = new IsSubinterval(
                buildIntervalList(["2015/2017", "2018/2020", "2011-01-01T14:00:00/2012-02-01"])
        )
        Interval test = new Interval(testInterval)

        expect:
        subinterval.test(test) == result

        where:
        result | testInterval
        true   | "2015/2016"
        true   | "2015-01-01/2016-12-31"
        false  | "2014/2015"
        false  | "2014/2016"
        true   | "2011-01-01T14:00:00/2012"
    }
}
