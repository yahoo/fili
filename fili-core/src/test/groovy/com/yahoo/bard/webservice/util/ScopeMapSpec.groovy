// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util

import groovy.transform.EqualsAndHashCode
import spock.lang.Specification

import javax.validation.constraints.NotNull

class ScopeMapSpec extends Specification {

        @EqualsAndHashCode
        public class ScopeMapImpl extends ScopeMap<String, String, Integer, ScopeMapImpl> {

        public ScopeMapImpl() {
            super()
        }

        public ScopeMapImpl(@NotNull ScopeMap<String, String, Integer, ScopeMapImpl> scope) {
            super(scope)
        }

        @Override
        protected ScopeMapImpl factory(ScopeMapImpl scope) {
            return new ScopeMapImpl(scope)
        }
    }

    def "Sub scope delegates to parent"() {
        setup:
        ScopeMapImpl scopeMap = new ScopeMapImpl()
        ScopeMap aScope = scopeMap.getScope('a')
        scopeMap.put('abe', 1)
        scopeMap.put('bob', 2)
        aScope.put('abe', 3)

        expect:
        aScope.get('bob') == 2
        aScope.get('abe') == 3
        scopeMap.get('abe') == 1
    }

    def "Scope put creates and populates new scope"() {
        setup:
        ScopeMapImpl scopeMap = new ScopeMapImpl()
        scopeMap.putScope('a', 'abe', 1)

        expect:
        scopeMap.getScope('a').get('abe') == 1
    }

    def "getScope Lazily initializes"() {
        setup:
        ScopeMapImpl scopeMap = new ScopeMapImpl()
        ScopeMap aScope = scopeMap.getScope('a')

        expect: "Object created in earlier step is exactly the one in the second call."
        aScope.is(scopeMap.getScope('a'))
    }

    def "getScope multiget resolves as a chaining simple get"() {
        setup:
        ScopeMapImpl scopeMap = new ScopeMapImpl()
        ScopeMap aScope = scopeMap.getScope('a')
        ScopeMap bScope = aScope.getScope('b')
        ScopeMap cScope = bScope.getScope('c')

        expect:
        scopeMap.getScope('a').is(aScope)
        scopeMap.getScope('a', 'b').is(bScope)
        scopeMap.getScope('a', 'b', 'c').is(cScope)
    }
}
