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

        ApiFilter t_dim2_filter1 = Mock()
        ApiFilter t_dim3_filter1 = Mock()
        ApiFilter r_dim1_filter1 = Mock()
        ApiFilter r_dim2_filter1 = Mock()
        ApiFilter r_dim2_filter2 = Mock()

        ApiFilters tableFilters = new ApiFilters(
                [
                        (dim2) : [t_dim2_filter1] as Set,
                        (dim3) : [t_dim3_filter1] as Set
                ] as Map
        )

        ApiFilters requestFilters = new ApiFilters(
                [
                        (dim1) : [r_dim1_filter1] as Set,
                        (dim2) : [r_dim2_filter1, r_dim2_filter2] as Set
                ] as Map
        )

        expect:
        ApiFilters.merge(tableFilters, requestFilters) == new ApiFilters(
                [
                        (dim1) : [r_dim1_filter1] as Set,
                        (dim2) : [r_dim2_filter1, r_dim2_filter2, t_dim2_filter1] as Set,
                        (dim3) : [t_dim3_filter1] as Set
                ] as Map
        )
    }
}
