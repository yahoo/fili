// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator.filter;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

/**
 * A method to bind a string representation of an api filter list into a concrete ApiFilters instance.
 */
public interface FilterGenerator {

    /**
     * Generates api filter objects on the based on the filter query vallue in the request parameters.
     *
     * @param filterQuery  A String description of a filter model
     * @param logicalTable  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     * @throws BadApiRequestException if the filter query string does not match required syntax, or the filter
     * contains a 'startsWith' or 'contains' operation while the BardFeatureFlag.DATA_STARTS_WITH_CONTAINS_ENABLED is
     * off.
     */
    ApiFilters generate(String filterQuery, LogicalTable logicalTable, DimensionDictionary dimensionDictionary)
            throws BadApiRequestException;
}
