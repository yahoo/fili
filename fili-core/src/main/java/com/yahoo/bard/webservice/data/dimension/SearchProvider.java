// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.web.ApiFilter;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

/**
 * Provides the capability to search through dimension metadata. For example, finding all dimensions whose name
 * contains "page_views."
 */
public interface SearchProvider {

    /**
     * Setter for dimension
     *
     * @param dimension  Dimension the SearchProvider is searching
     */
    void setDimension(Dimension dimension);

    /**
     * Setter for store
     *
     * @param keyValueStore  KeyValueStore that holds the data rows indexed by the Search Provider
     */
    void setKeyValueStore(KeyValueStore keyValueStore);

    /**
     * Gets the number of distinct dimension rows (assuming the key field is unique) in the index
     *
     * @return The number of dimension rows for this dimension
     */
    int getDimensionCardinality();

    /**
     * Getter for dimension rows
     *
     * @return Set of dimension rows the Search Provider has in it's indexes
     */
    Set<DimensionRow> findAllDimensionRows();

    /**
     * Getter for dimension rows in tree set for consistent order
     *
     * @return tree set of dimension rows the Search Provider has in it's indexes
     */
    TreeSet<DimensionRow> findAllOrderedDimensionRows();

    /**
     * Get a set of dimension row(s) given the Dimension Field and its value
     *
     * @param dimensionField  DimensionField to search
     * @param fieldValue  Value in the DimensionField that we're searching for
     *
     * @return set of dimension row(s). For find by the Dimension's Key field, a set is returned with a single row
     *
     * @deprecated Vestigial, and may predate doing the filtering through the SearchProviders.
     */
    @Deprecated
    Set<DimensionRow> findAllDimensionRowsByField(@NotNull DimensionField dimensionField, String fieldValue);

    /**
     * Get a set of dimension row(s) given a set of ApiFilters
     *
     * @param filters  ApiFilters to use for finding matching dimension rows
     *
     * @return set of dimension row(s)
     */
    TreeSet<DimensionRow> findFilteredDimensionRows(Set<ApiFilter> filters);

    /**
     * Method to add / update indexes
     *
     * @param rowId  Unique ID for the dimension row
     * @param dimensionRow  New, updated dimension row
     * @param dimensionRowOld  Old dimension row if we're updating one, or null if there is no row for the rowId
     */
    void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld);

    /**
     * Method to add / update indexes
     *
     * @param changedRows Collection of newRow / oldRow pairs keyed by rowId to update the index with. Each row is a
     * pair of dimension rows (newRow is the key, oldRow is the value), with the rowId (ie. unique key in the dimension)
     * as the top-level Map key.
     */
    void refreshIndex(Map<String, Pair<DimensionRow, DimensionRow>> changedRows);

    /**
     * Method to check if search provider is healthy
     *
     * @return true if healthy
     */
    boolean isHealthy();

    /**
     * Clears the dimension cache, and resets the indices, effectively resetting the SearchProvider to a clean state.
     */
    void clearDimension();
}
