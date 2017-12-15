// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.builders.DruidOrFilterBuilder
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl
import com.yahoo.bard.webservice.web.filters.ApiFilters
<<<<<<< 298fce66f1668fcfde4f429ae6eafa21d00ac9ee
import com.yahoo.bard.webservice.web.util.PaginationParameters
=======
>>>>>>> temp

import org.joda.time.DateTimeZone

class TestingDataApiRequestImpl extends DataApiRequestImpl {

    TestingDataApiRequestImpl() {
        super(
                (LogicalTable) null,
                DefaultTimeGrain.DAY,
<<<<<<< 298fce66f1668fcfde4f429ae6eafa21d00ac9ee
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
=======
                [] as Set,
                null,
                [] as Set,
                [] as Set,
                [:] as ApiFilters,
                null,
                null,
                null,
                0,
                0,
                Long.MAX_VALUE,
                null,
                null,
                null,
                new DefaultDruidFilterBuilder()
>>>>>>> temp
        )
    }
}
