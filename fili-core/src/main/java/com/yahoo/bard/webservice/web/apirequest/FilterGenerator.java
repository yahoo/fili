// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

/**
 * A Single Abstract Method interface describing a function to produce an ApiFilters object given a String from the
 * query.
 */
public interface FilterGenerator {

    /**
     * A function to produce api filters from query parameters.
     *
     * @param filterQuery  An api filter expression
     * @param logicalTable  The logical table to bind against
     * @param dimensionDictionary  The dimension dictionary to bind against
     *
     * @return  Filters suitable for a query or filtered dimension
     */
    ApiFilters generate(String filterQuery, LogicalTable logicalTable, DimensionDictionary dimensionDictionary);
}
