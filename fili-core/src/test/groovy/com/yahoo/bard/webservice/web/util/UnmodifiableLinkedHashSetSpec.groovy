// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util

import com.yahoo.bard.webservice.util.UnmodifiableLinkedHashSet

import spock.lang.Specification

class UnmodifiableLinkedHashSetSpec extends Specification {

    List<String> words
    LinkedHashSet<String> target
    UnmodifiableLinkedHashSet<String> u

    def setup() {
        words = ["The", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog"]
        target = new LinkedHashSet<>()
        target.addAll(words)

        u = UnmodifiableLinkedHashSet.of(target)
    }

    def "wrapped list order matches target order"() {
        setup:
        Iterator<String> ui = u.iterator()
        Iterator<String> ti = target.iterator()

        expect:
        while(ui.hasNext()) {
            assert ti.next() == ui.next()
        }
        !ti.hasNext()
    }

    def "can't add or remove anything to wrapped set"() {
        setup:
        LinkedHashSet<String> backup = new LinkedHashSet<>(u)

        when:
        u.add("BAD")

        then:
        thrown(UnsupportedOperationException)

        when:
        u.addAll(["BAD1", "BAD2"])

        then:
        thrown(UnsupportedOperationException)

        when:
        u.remove("The")

        then:
        thrown(UnsupportedOperationException)

        when:
        u.removeAll(["The", "quick"])

        then:
        thrown(UnsupportedOperationException)

        when:
        u.clear()

        then:
        thrown(UnsupportedOperationException)

        and: "nothing was changed"
        u == backup
    }

    def "mutating target set does NOT mutate wrapped set"() {
        setup:
        LinkedHashSet<String> expected = new LinkedHashSet<>(words)

        expect:
        u == target
        u == expected

        when:
        target.removeAll(["The", "quick"])

        then:
        u == expected
        u != target
    }

    def "remove on iterator is not supported"() {
        setup:
        Iterator<String> i = u.iterator()
        i.next()

        when:
        i.remove()

        then:
        thrown(UnsupportedOperationException)
    }
}
