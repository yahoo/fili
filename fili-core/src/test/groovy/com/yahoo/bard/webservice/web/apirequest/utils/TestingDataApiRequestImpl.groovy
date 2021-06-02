// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.utils


import com.yahoo.bard.webservice.data.time.DefaultTimeGrain
import com.yahoo.bard.webservice.druid.model.orderby.OrderByColumn
import com.yahoo.bard.webservice.table.LogicalTable
import com.yahoo.bard.webservice.web.ResponseFormatType
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestImpl
import com.yahoo.bard.webservice.web.apirequest.ProtocolMetricDataApiReqestImpl
import com.yahoo.bard.webservice.web.apirequest.generator.metric.ProtocolLogicalMetricGenerator
import com.yahoo.bard.webservice.web.apirequest.metrics.ApiMetricAnnotater
import com.yahoo.bard.webservice.web.filters.ApiFilters
import com.yahoo.bard.webservice.web.util.PaginationParameters

import org.apache.commons.collections4.MultiValuedMap
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap
import org.joda.time.DateTimeZone

class TestingDataApiRequestImpl {
    static List<String> protocols = Collections.emptyList();

    ProtocolLogicalMetricGenerator protocolLogicalMetricGenerator = new ProtocolLogicalMetricGenerator(
            ApiMetricAnnotater.NO_OP_ANNOTATER,
            protocols
    );

    public static ProtocolMetricDataApiReqestImpl buildDataApiRequestValue() {
        buildDataApiRequestValue(new ArrayListValuedHashMap<String, String>())
    }

    public static ProtocolMetricDataApiReqestImpl buildDataApiRequestValue(MultiValuedMap<String, String> queryParams) {

        ProtocolMetricDataApiReqestImpl request =  new ProtocolMetricDataApiReqestImpl(
                (LogicalTable) null,
                DefaultTimeGrain.DAY,
                [] as LinkedHashSet, // Dimensions
                null, // perDimensionFields
                [] as LinkedHashSet, // LogicalMetrics
                [], // Intervals
                (ApiFilters) null,
                [:], // Havings
                null, // sorts
                (OrderByColumn) null, //dateTimeSort
                (DateTimeZone) DateTimeZone.UTC,
                0, // topN
                0, // count
                (PaginationParameters) null,
                (ResponseFormatType) null,
                "", // filename
                Long.MAX_VALUE, // asyncAfter
                true,
                queryParams
        )
        request.metricBinder = new ProtocolLogicalMetricGenerator(
                ApiMetricAnnotater.NO_OP_ANNOTATER,
                protocols
        );
        return request
    }

    public static DataApiRequestImpl buildStableDataApiRequestImpl() {

        return new DataApiRequestImpl(
                (LogicalTable) null,
                DefaultTimeGrain.DAY,
                [] as LinkedHashSet, // Dimensions
                null, // perDimensionFields
                [] as LinkedHashSet, // LogicalMetrics
                [], // Intervals
                (ApiFilters) null,
                [:], // Havings
                null, // sorts
                (OrderByColumn) null, //dateTimeSort
                (DateTimeZone) DateTimeZone.UTC,
                0, // topN
                0, // count
                (PaginationParameters) null,
                (ResponseFormatType) null,
                "",
                Long.MAX_VALUE, // asyncAfter
                true
        )
    }
}
