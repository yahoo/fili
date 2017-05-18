// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.util.ClassScanner
import com.yahoo.bard.webservice.web.DataApiRequest

import spock.lang.Specification

class QueryPlanningConstraintSpec extends Specification {

    def supplyDependencies() {
        {
            ClassScanner classScanner, Class aClass ->
            DataApiRequest dataApiRequest = Mock(DataApiRequest)
            dataApiRequest.getDimensions() >> ([] as Set)
            dataApiRequest.getFilterDimensions() >> ([] as Set)
            dataApiRequest.getFilters() >> [:]
            dataApiRequest.getIntervals() >> ([] as Set)
            dataApiRequest.getLogicalMetrics() >> ([] as Set)
            dataApiRequest.getGranularity() >> (DefaultTimeGrain.DAY)
            classScanner.putInArgumentValueCache(DataApiRequest.class, dataApiRequest)
        }
    }
}
