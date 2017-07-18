package com.yahoo.fili.webservice.logging.blocks

import com.yahoo.fili.webservice.data.dimension.Dimension
import com.yahoo.fili.webservice.druid.model.filter.AndFilter
import com.yahoo.fili.webservice.druid.model.filter.Filter
import com.yahoo.fili.webservice.druid.model.filter.NotFilter
import com.yahoo.fili.webservice.druid.model.filter.OrFilter
import com.yahoo.fili.webservice.druid.model.filter.SelectorFilter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class DruidFilterInfoSpec extends Specification {

    @Shared Filter selector = new SelectorFilter(Stub(Dimension), "value")

    static final String SELECTOR_NAME = SelectorFilter.class.simpleName
    static final String AND_NAME = AndFilter.class.simpleName
    static final String OR_NAME = OrFilter.class.simpleName
    static final String NOT_NAME = NotFilter.class.simpleName

    @Unroll
    def "The DruidFilterInfo correctly analyzes #filter"() {
        expect:
        new DruidFilterInfo(filter).numEachFilterType == expectedMap

        where:
        filter                                                                                                  | expectedMap
        null                                                                                                    | [:]
        selector                                                                                                | [(SELECTOR_NAME): 1L]
        new AndFilter([selector, selector])                                                                     | [(AND_NAME): 1L, (SELECTOR_NAME): 2L]
        new NotFilter(new AndFilter([selector, selector]))                                                      | [(NOT_NAME): 1L, (AND_NAME): 1L, (SELECTOR_NAME): 2L]
        new OrFilter([new AndFilter([selector, selector]), new NotFilter(new AndFilter([selector, selector]))]) | [(OR_NAME): 1L, (AND_NAME): 2L, (NOT_NAME): 1L, (SELECTOR_NAME): 4L]
    }
}
