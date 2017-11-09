// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.filter

import com.yahoo.bard.webservice.data.dimension.Dimension

import spock.lang.Specification

class SelectorFilterSpec extends Specification {
    def "withDimension() rejects null Dimension"() {
        given: "a SelectorFilter"
        SelectorFilter selectorFilter = new SelectorFilter(Mock(Dimension), "")

        when: "we set the filter with a null Dimension"
        selectorFilter.withDimension(null)

        then: "we get an exception"
        thrown(NullPointerException)
    }
}
