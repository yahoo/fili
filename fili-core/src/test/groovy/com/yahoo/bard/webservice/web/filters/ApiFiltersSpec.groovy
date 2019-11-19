// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.filters

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.web.ApiFilter

import spock.lang.Specification

class ApiFiltersSpec extends Specification {

    def "ApiFilters merge properly"() {
        setup:
        Dimension dim1 = Mock()
        Dimension dim2 = Mock()
        Dimension dim3 = Mock()

        ApiFilter f1_dim2_filter1 = Mock()
        ApiFilter f1_dim3_filter1 = Mock()
        ApiFilter f2_dim1_filter1 = Mock()
        ApiFilter f2_dim2_filter1 = Mock()
        ApiFilter f2_dim2_filter2 = Mock()

        ApiFilters apiFilters1 = new ApiFilters(
                [
                        (dim2) : [f1_dim2_filter1] as Set,
                        (dim3) : [f1_dim3_filter1] as Set
                ] as Map
        )

        ApiFilters apiFilters2 = new ApiFilters(
                [
                        (dim1) : [f2_dim1_filter1] as Set,
                        (dim2) : [f2_dim2_filter1, f2_dim2_filter2] as Set
                ] as Map
        )

        expect:
        ApiFilters.union(apiFilters1, apiFilters2) == new ApiFilters(
                [
                        (dim1) : [f2_dim1_filter1] as Set,
                        (dim2) : [f2_dim2_filter1, f2_dim2_filter2, f1_dim2_filter1] as Set,
                        (dim3) : [f1_dim3_filter1] as Set
                ] as Map
        )
    }
}
