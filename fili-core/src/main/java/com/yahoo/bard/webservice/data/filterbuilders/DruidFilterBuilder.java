// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.filterbuilders;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionRowNotFoundException;
import com.yahoo.bard.webservice.druid.model.filter.Filter;
import com.yahoo.bard.webservice.web.ApiFilter;

import java.util.Map;
import java.util.Set;

/**
 * Builds druid query model objects from ApiFilters.
 */
@FunctionalInterface
public interface DruidFilterBuilder {

    /**
     * Combines the filters for a set of dimensions into a single Druid filter.
     *
     * @param filterMap  The map of filters per dimension
     *
     * @return A filter that combines the filters for each of the dimensions, or null if there is no filter
     *
     * @throws DimensionRowNotFoundException if filtering on a dimension that does not have dimension rows
     */
    Filter buildFilters(Map<Dimension, Set<ApiFilter>> filterMap) throws DimensionRowNotFoundException;
}
