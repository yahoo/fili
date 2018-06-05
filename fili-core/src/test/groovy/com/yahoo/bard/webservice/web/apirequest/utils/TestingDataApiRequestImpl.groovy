// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.filterbuilders.DefaultDruidFilterBuilder
import com.yahoo.bard.webservice.data.metric.LogicalMetric
import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl
import com.yahoo.bard.webservice.web.filters.ApiFilters
import com.yahoo.bard.webservice.web.util.PaginationParameters

import org.joda.time.Interval

class TestingDataApiRequestImpl extends DataApiRequestImpl {
    TestingDataApiRequestImpl() {
        super(
                (ResponseFormatType) null,
                (Optional<PaginationParameters>) null,
                (LogicalTable) null,
                DefaultTimeGrain.DAY,
                (Set<Dimension>) [] as Set,
                null,
                (Set<LogicalMetric>) [] as Set,
                (Set<Interval>) [] as Set,
                (ApiFilters) [:],
                null,
                null,
                null,
                0,
                0,
                Long.MAX_VALUE,
                null,
                new DefaultDruidFilterBuilder(),
                null,
                null
        )
    }
}
