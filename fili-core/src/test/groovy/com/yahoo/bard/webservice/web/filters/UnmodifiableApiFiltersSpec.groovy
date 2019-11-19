// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.util.UnmodifiableLinkedHashMap
import com.yahoo.bard.webservice.web.ApiFilter

import spock.lang.Specification

class UnmodifiableApiFiltersSpec extends Specification {

    Dimension dim1
    Dimension dim2

    Map<Dimension, Set<ApiFilters>> filters
    ApiFilters target
    UnmodifiableApiFilters u

    def setup() {
        dim1 = Mock(Dimension) { getApiName() >> "dim1"}
        dim2 = Mock(Dimension) { getApiName() >> "dim2"}

        filters = [
                (dim1): [] as Set,
                (dim2): [] as Set,
        ] as Map
        target = new ApiFilters(filters)

        u = UnmodifiableApiFilters.of(target)
    }

    def "wrapped list order matches target order"() {
        setup:
        Iterator<Map.Entry<Dimension, Set<ApiFilter>>> ui = u.entrySet().iterator()
        Iterator<Map.Entry<Dimension, Set<ApiFilter>>> ti = target.entrySet().iterator()

        expect:
        while(ui.hasNext()) {
            assert ui.next() == ti.next()
        }
        !ti.hasNext()
    }

    def "can't add or remove anything to wrapped set"() {
        setup:
        ApiFilters backup = new ApiFilters(u)

        when:
        u.put(Mock(Dimension), [] as Set)

        then:
        thrown(UnsupportedOperationException)

        when:
        u.putAll([(Mock(Dimension)): [] as Set])

        then:
        thrown(UnsupportedOperationException)

        when:
        u.remove(dim1)

        then:
        thrown(UnsupportedOperationException)

        when:
        u.replace(dim1, [] as Set)

        then:
        thrown(UnsupportedOperationException)

        when:
        u.replaceAll({key, old -> [] as Set})

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
        ApiFilters expected = new ApiFilters(filters)

        expect:
        u == target
        u == expected

        when:
        target.remove(dim1)

        then:
        u == expected
        u != target
    }

    def "remove on iterators are not supported"() {
        setup:
        Iterator<Map.Entry<Dimension, Set<ApiFilter>>> es = u.entrySet().iterator()
        Iterator<Dimension> ks = u.keySet().iterator()
        Iterator<Set<ApiFilter>> vs = u.values().iterator()

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

    def "immutableUnion successfully unions two api filters"() {
        setup:
        ApiFilter f1 = Mock(ApiFilter)
        ApiFilter f2 = Mock(ApiFilter)

        ApiFilters filters1 = new ApiFilters([(dim1): [f1] as Set])
        ApiFilters filters2 = new ApiFilters([(dim2): [f2] as Set])

        when:
        ApiFilters result = UnmodifiableApiFilters.immutableUnion(filters1, filters2)

        then:
        result == [
                (dim1): [f1] as Set,
                (dim2): [f2] as Set,
        ] as ApiFilters
    }
}
