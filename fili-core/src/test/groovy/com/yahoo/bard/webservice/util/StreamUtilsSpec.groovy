// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TWO_VALUES_OF_THE_SAME_KEY

import spock.lang.Specification

import java.util.function.Predicate
import java.util.stream.Stream

class StreamUtilsSpec extends Specification {
    def "When duplicate keys merge, IllegalStateException is thrown"() {
        when: "a key, which is already associates with a value, is inserted to a map"
        [dupKey: 1].merge("dupKey", 2, StreamUtils.throwingMerger())

        then: "insert if rejected"
        IllegalStateException exception = thrown()
        exception.message == TWO_VALUES_OF_THE_SAME_KEY.format(1, 2)
    }

    def "Stream collected to unmodifiable set cannot be modified and element first-in ordering is preserved"() {
        given: "a stream collected to an unmodifiable set"
        Set<Integer> set = StreamUtils.toUnmodifiableSet(Stream.of(1, 2, 3))
        Iterator<Integer> iterator = set.iterator()

        expect: "elements are pulled out in-order"
        iterator.next() == 1
        iterator.next() == 2
        iterator.next() == 3
        !iterator.hasNext()

        when: "we try to modify by adding an element to the set"
        set.add(4)

        then: "it is rejected"
        thrown(UnsupportedOperationException)
    }

    def "Negating a Predicate reverses test result"() {
        given: "a predicate whose test returns true"
        Predicate<Object> predicate = Mock(Predicate) {test() >> true}

        when: "we negate that predicate"
        StreamUtils.not(predicate)

        then: "test returns false"
        !predicate.test()
    }

    def "Appending returns set with the appended element"() {
        expect:
        StreamUtils.append([1, 2] as Set, 3) == [1, 2, 3] as Set
    }

    def "Merging returns set containing two sets"() {
        expect:
        StreamUtils.setMerge([1, 2, 3] as Set, [4, 5, 6] as Set) == [1, 2, 3, 4, 5, 6] as Set
    }

    def "Merging returns an ordered set containing two sets"() {
        expect:
        StreamUtils.orderedSetMerge([1,2,7] as LinkedHashSet, [3,6,9] as LinkedHashSet) == [1,2,7,3,6,9] as Set
    }
}
