// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import spock.lang.Specification

/**
 * Tests for the Delegating Map
 */
class DelegatingMapSpec extends Specification {

    Map innermostMap
    DelegatingMap midMap
    DelegatingMap topMap

    def setup() {
        innermostMap = new LinkedHashMap()
        innermostMap.putAll([a: 3, b: 3, c: 3, e: 3])

        midMap = new DelegatingMap(innermostMap)
        midMap.putAll([a: 2, b: 2, d: 2])
        topMap = new DelegatingMap(midMap)
        topMap.putAll([a: 1, e: 1, f: 1])
    }

    def "Test delegation returns value from correct level"() {
        expect:
        topMap.get('a') == 1
        topMap.get('b') == 2
        topMap.get('c') == 3
        topMap.get('d') == 2
        topMap.get('e') == 1
        topMap.get('f') == 1
        topMap.get('g') == null
    }

    def "Clear throws exception"() {
        when:
        topMap.clear()

        then:
        thrown(UnsupportedOperationException)
    }

    def "ClearLocal wipes out all top level entries"() {
        when:
        topMap.clearLocal()

        then:
        topMap.get('a') == 2
        topMap.get('b') == 2
        topMap.get('c') == 3
        topMap.get('d') == 2
        topMap.get('e') == 3
        topMap.get('f') == null
        topMap.get('g') == null
    }

    def "ContainsKey corresponds to visible entry keys"() {
        expect:
        topMap.containsKey('a')
        topMap.containsKey('b')
        topMap.containsKey('c')
        topMap.containsKey('d')
        topMap.containsKey('e')
        topMap.containsKey('f')
        ! topMap.containsKey('g')
    }

    def "ContainsValue returns available values"() {
        expect:
        topMap.containsValue(1)
        topMap.containsValue(2)
        topMap.containsValue(3)

        ! midMap.containsValue(1)
        midMap.containsValue(2)
        midMap.containsValue(3)

        when:
        midMap.clearLocal()

        then:
        topMap.containsValue(1)
        ! topMap.containsValue(2)
        topMap.containsValue(3)

        ! midMap.containsValue(1)
        ! midMap.containsValue(2)
        midMap.containsValue(3)

        when:
        topMap.clearLocal()

        then:
        ! topMap.containsValue(1)
        ! topMap.containsValue(2)
        topMap.containsValue(3)
    }

    def "Keyset has right values in right order"() {
        expect:
        topMap.keySet() as List == ['c', 'b', 'd', 'a', 'e', 'f']
    }

    def "Entry stream is in canonical order"() {
        List<List<?>> expected = [['c', 3], ['b', 2], ['d', 2], ['a', 1], ['e', 1], ['f', 1]]

        expect:
        topMap.entrySet().collect {[it.key, it.value]} == expected

    }

    def "Put correctly overshadows"() {
        setup:
        int oldValue = topMap.put('c', 4)

        expect:
        oldValue == 3
        topMap.get('c') == 4
    }

    def "Remove throws exception"() {
        when:
        topMap.remove('a')

        then:
        thrown(UnsupportedOperationException)
    }

    def "RemoveLocal reveals shadowed value from delegate"() {
        setup:
        topMap.removeLocal('a')

        expect:
        topMap.get('a') == 2
    }

    def "Size increases and decreases based on count of top level keys"() {
        expect: "Default size"
        topMap.size() == 6

        when: "Overshadowing doesn't increase size"
        topMap.put('b', 2)

        then:
        topMap.size() == 6

        when: "Removing an unshadowed value lower the size"
        topMap.removeLocal('f')

        then:
        topMap.size() == 5

        when: "Adding an unshadowed value increases size"
        topMap.put('z', 12)

        then:
        topMap.size() == 6
    }

    def "Values contains correct values"() {
        expect:
        [1, 2, 3] as Set == topMap.values()
    }

    def "Flat view has a view of data"() {
        expect:
        topMap.flatView() == [c: 3, b: 2, d: 2, a: 1, e: 1, f: 1]
    }
}
