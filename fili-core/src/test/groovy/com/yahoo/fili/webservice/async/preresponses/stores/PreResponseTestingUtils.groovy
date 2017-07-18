// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.async.preresponses.stores

import static com.yahoo.fili.webservice.data.time.DefaultTimeGrain.DAY

import com.yahoo.fili.webservice.data.Result
import com.yahoo.fili.webservice.data.ResultSet
import com.yahoo.fili.webservice.data.ResultSetSchema
import com.yahoo.fili.webservice.data.dimension.Dimension
import com.yahoo.fili.webservice.data.dimension.DimensionColumn
import com.yahoo.fili.webservice.data.dimension.DimensionField
import com.yahoo.fili.webservice.data.dimension.DimensionRow
import com.yahoo.fili.webservice.data.metric.MetricColumn
import com.yahoo.fili.webservice.web.PreResponse
import com.yahoo.fili.webservice.web.responseprocessors.ResponseContext

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

        ResultSetSchema schema = new ResultSetSchema(DAY, Collections.emptySet())

        return new PreResponse(new ResultSet(schema, [rs]), responseContext)
    }
}
