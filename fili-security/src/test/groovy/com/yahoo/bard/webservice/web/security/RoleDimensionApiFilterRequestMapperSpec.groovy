// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.security

import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.web.ApiFilter

import spock.lang.Specification

class RoleDimensionApiFilterRequestMapperSpec extends Specification {

    ApiFilter security1 = Mock(ApiFilter)
    ApiFilter security2 = Mock(ApiFilter)
    Set<ApiFilter> securitySet = [security1, security2] as Set
    ResourceDictionaries dictionaries = Mock(ResourceDictionaries)
    Dimension filterDimension = Mock(Dimension)
    Dimension nonFilterDimension = Mock(Dimension)

    RoleDimensionApiFilterRequestMapper mapper = new RoleDimensionApiFilterRequestMapperSpec(dictionaries, )

    def "Test substituteFilters merges on matching dimension and not otherwise"() {
        RoleDimensionApiFilterRequestMapper mapper = new RoleDimensionApiFilterRequestMapperSpec()

    }

    def "InternalApply"() {
    }

    def "SubstituteFilters"() {
    }


    def "UnionMergeFilterValues"() {
    }
}
