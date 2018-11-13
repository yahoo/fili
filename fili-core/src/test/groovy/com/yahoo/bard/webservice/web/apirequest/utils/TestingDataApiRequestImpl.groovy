// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder
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
                [] as LinkedHashSet, // Dimensions
                null, // perDimensionFields
                [] as LinkedHashSet,  // LogicalMetrics
                [], // Intervals
                (ApiFilters) null,
                [:], // Havings
                null, // sorts
                Optional.empty(),
                (DateTimeZone) null,
                0, // topN
                0,  // count
                (Optional<PaginationParameters>) null,
                (ResponseFormatType) null,
                Long.MAX_VALUE, // asynchAfter
                new DruidOrFilterBuilder()
        )
    }
}
