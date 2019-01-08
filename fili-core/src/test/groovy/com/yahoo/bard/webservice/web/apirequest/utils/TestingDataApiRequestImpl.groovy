// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl
import com.yahoo.bard.webservice.web.filters.ApiFilters
import com.yahoo.bard.webservice.web.util.PaginationParameters

import org.joda.time.DateTimeZone

class TestingDataApiRequestImpl extends DataApiRequestImpl {

    TestingDataApiRequestImpl() {
        super(
                (LogicalTable) null,
                DefaultTimeGrain.DAY,
                new LinkedHashSet<>(), // Dimensions
                (LinkedHashMap) null, // perDimensionFields
                new LinkedHashSet<>(),  // LogicalMetrics
                [], // Intervals
                (ApiFilters) null,
                [:], // Havings
                new LinkedHashSet<>(), // sorts
                Optional.empty(),
                (DateTimeZone) null,
                (int) 0, // topN
                (int) 0,  // count
                (PaginationParameters) null,
                (ResponseFormatType) null,
                (String) null, // filename
                Long.MAX_VALUE // asynchAfter
        )
    }
}

