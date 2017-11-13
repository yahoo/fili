// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter

import com.yahoo.bard.webservice.data.dimension.Dimension

import spock.lang.Specification

class DimensionalFilterSpec extends Specification {
    private class TestingDimensionalFilter extends DimensionalFilter {
        TestingDimensionalFilter(Dimension dimension, FilterType type) {
            super(dimension, type)
        }

        TestingDimensionalFilter withDimension(Dimension dimension) {
            return null
        }
    }

    def "Constructor rejects null Dimension"() {
        when: "we try to create a DimensionFilter with a null Dimension"
        new TestingDimensionalFilter(null, Mock(FilterType))

        then: "we get an exception"
        thrown(NullPointerException)
    }
}
