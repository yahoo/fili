// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.filters.ApiFilters;

/**
 * A method to bind a string representation of an api filter list into a concrete ApiFilters instance.
 */
@Incubating
public interface FilterGenerator {

    /**
     * Default implementation of this interface.
     */
    FilterGenerator DEFAULT_FILTER_GENERATOR = new FilterBinders();

    /**
     * Generates api filter objects on the based on the filter query vallue in the request parameters.
     *
     * @param filterQuery  A String description of a filter model
     * @param logicalTable  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     *
     * @deprecated generateFilters was first added to the public API as just generate. The name was changed for clarity,
     * and the new version should be used instead. This method is kept on the public api to keep the name change from
     * breaking existing code. Please use the newer version.
     */
    @Deprecated
    default ApiFilters generate(
            String filterQuery,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    ) {
        return generateFilters(filterQuery, logicalTable, dimensionDictionary);
    }

    /**
     * Generates api filter objects on the based on the filter query vallue in the request parameters.
     *
     * @param filterQuery  A String description of a filter model
     * @param logicalTable  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     *
     * @return Set of filter objects.
     */
    ApiFilters generateFilters(String filterQuery, LogicalTable logicalTable, DimensionDictionary dimensionDictionary);

    /**
     * Validated bound api filter objects.
     *
     * @param filterQuery  A String description of a filter model
     * @param apiFilters  Bound api filters
     * @param logicalTable  The logical table for the data request
     * @param dimensionDictionary  DimensionDictionary
     */
    void validateApiFilters(
            String filterQuery,
            ApiFilters apiFilters,
            LogicalTable logicalTable,
            DimensionDictionary dimensionDictionary
    );
}
