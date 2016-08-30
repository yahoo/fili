// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.async.preresponses.stores

import static com.yahoo.bard.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.bard.webservice.data.Result
import com.yahoo.bard.webservice.data.ResultSet
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.DimensionColumn
import com.yahoo.bard.webservice.data.dimension.DimensionField
import com.yahoo.bard.webservice.data.dimension.DimensionRow
import com.yahoo.bard.webservice.data.metric.MetricColumn
import com.yahoo.bard.webservice.table.Schema
import com.yahoo.bard.webservice.web.PreResponse
import com.yahoo.bard.webservice.web.responseprocessors.ResponseContext

import org.joda.time.DateTime

/**
 * A class containing utility methods to help test implementations of PreResponseStore.
 */
class PreResponseTestingUtils {

    /**
     * Build a PreResPonse object using the given date. Please note, the PreResponse object returned is more or less a
     * stub i.e. it contains empty dimensionRows, metricValues, dimensionToDimensionFieldMap and responseContext. Only
     * DateTime and Schema are meaningful objects.
     *
     * @param date  The Date to be used to build PreResponse.
     * @return A stubby PreResponse object.
     */
    public static PreResponse buildPreResponse(String date) {
        Map<DimensionColumn, DimensionRow> dimensionRow = new HashMap<>()
        Map<MetricColumn, BigDecimal> metricValues = new HashMap<>()
        Result rs = new Result(dimensionRow, metricValues, DateTime.parse(date))

        Map<Dimension, Set<DimensionField>> dimensionToDimensionFieldMap = new HashMap<>()
        ResponseContext responseContext = new ResponseContext(dimensionToDimensionFieldMap)

        Schema schema = new Schema(DAY)

        return new PreResponse(new ResultSet([rs], schema), responseContext)
    }
}
