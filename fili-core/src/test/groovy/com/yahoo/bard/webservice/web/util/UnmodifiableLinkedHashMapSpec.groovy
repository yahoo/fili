// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util

import com.yahoo.bard.webservice.util.UnmodifiableLinkedHashMap

import spock.lang.Specification

class UnmodifiableLinkedHashMapSpec extends Specification {

    Map<String, Integer> words
    LinkedHashMap<String, Integer> target
    UnmodifiableLinkedHashMap<String, Integer> u

    def setup() {
        words = [
                The: 1,
                quick: 2,
                brown: 3,
                fox: 4,
                jumped: 5,
                over: 6,
                the: 7,
                lazy: 8,
                dog: 9,
        ]
        target = new LinkedHashMap<>(words)

        u = UnmodifiableLinkedHashMap.of(target)
    }

    def "wrapped list order matches target order"() {
        setup:
        Iterator<Map.Entry<String, Integer>> ui = u.entrySet().iterator()
        Iterator<Map.Entry<String, Integer>> ti = target.entrySet().iterator()

        expect:
        while(ui.hasNext()) {
            assert ui.next() == ti.next()
        }
        !ti.hasNext()
    }

    def "can't add or remove anything to wrapped set"() {
        setup:
        LinkedHashMap<String, Integer> backup = new LinkedHashMap<>(u)

        when:
        u.put("BAD", 10)

        then:
        thrown(UnsupportedOperationException)

        when:
        u.putAll([
                BAD1: 10,
                BAD2: 11
        ])

        then:
        thrown(UnsupportedOperationException)

        when:
        u.remove("The")

        then:
        thrown(UnsupportedOperationException)

        when:
        u.replace("The", 1, 999)

        then:
        thrown(UnsupportedOperationException)

        when:
        u.replaceAll({key, old -> 999})

        then:
        thrown(UnsupportedOperationException)

        when:
        u.clear()

        then:
        thrown(UnsupportedOperationException)

        and:
        u == backup
    }

    def "mutating target set does NOT mutate wrapped set"() {
        setup:
        LinkedHashMap<String, Integer> expected = new LinkedHashMap<>(words)

        expect:
        u == target
        u == expected

        when:
        target.remove("The")

        then:
        u == expected
        u != target
    }

    def "remove on iterators are not supported"() {
        setup:
        Iterator<Map.Entry<String, Integer>> es = u.entrySet().iterator()
        Iterator<String> ks = u.keySet().iterator()
        Iterator<Integer> vs = u.values().iterator()

        when:
        es.next()
        es.remove()

        then:
        thrown(UnsupportedOperationException)

        when:
        ks.next()
        ks.remove()

        then:
        thrown(UnsupportedOperationException)

        when:
        vs.next()
        vs.remove()

        then:
        thrown(UnsupportedOperationException)
    }
}
