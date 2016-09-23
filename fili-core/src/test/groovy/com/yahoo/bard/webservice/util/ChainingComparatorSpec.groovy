// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import spock.lang.Specification
import spock.lang.Unroll

class ChainingComparatorSpec extends Specification {

    static Comparator<Integer> zero
    static Comparator<Integer> less
    static Comparator<Integer> more

    def setupSpec() {
        zero = Mock(Comparator)
        less = Mock(Comparator)
        more = Mock(Comparator)

        zero.compare(_, _) >> 0
        less.compare(_, _) >> -1
        more.compare(_, _) >> 1
    }

    @Unroll
    def "Test chain comparator with #comparatorList returns #expected"() {
        setup:
        ChainingComparator<Integer> chainingComparator = new ChainingComparator<>(comparatorList)

        expect:
        chainingComparator.compare(1, 2) == expected

        where:
        comparatorList      | expected
        [less]              | -1
        [zero]              |  0
        [more]              |  1
        [less, zero, more]  | -1
        [more, zero, less]  | 1
        [zero, more, less]  | 1
        [zero, less, more]  | -1
        [zero, zero, zero]  | 0
        []                  | 0
    }
}
