// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import javax.validation.constraints.NotNull;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Provides the capability to search through dimension metadata. For example, finding all dimensions whose name
 * contains "page_views."
 */
public interface SearchProvider {

    /**
     * Setter for dimension.
     *
     * @param dimension  Dimension the SearchProvider is searching
     */
    void setDimension(Dimension dimension);

    /**
     * Setter for store.
     *
     * @param keyValueStore  KeyValueStore that holds the data rows indexed by the Search Provider
     */
    void setKeyValueStore(KeyValueStore keyValueStore);

    /**
     * Gets the number of distinct dimension rows (assuming the key field is unique) in the index.
     *
     * @return The number of dimension rows for this dimension
     */
    int getDimensionCardinality();

    /**
     * Getter for dimension rows.
     *
     * @return Set of dimension rows the Search Provider has in it's indexes
     *
     * @deprecated  Searching for dimension rows is moving to a paginated version
     * ({@link #findAllDimensionRowsPaged}) in order to give greater control to the caller.
     */
    @Deprecated
    default Set<DimensionRow> findAllDimensionRows() {
        return new LinkedHashSet<>(
                findAllDimensionRowsPaged(PaginationParameters.EVERYTHING_IN_ONE_PAGE).getPageOfData()
        );
    }

    /**
     * Return the desired page of dimension rows.
     *
     * @param paginationParameters  The parameters that define a page (i.e. the number of rows per page,
     * and the page of results desired)
     *
     * @return A page of the dimension rows contained in the Search Provider's indices
     */
    Pagination<DimensionRow> findAllDimensionRowsPaged(@NotNull PaginationParameters paginationParameters);

    /**
     * Getter for dimension rows in tree set for consistent order.
     *
     * @return tree set of dimension rows the Search Provider has in it's indexes
     */
    TreeSet<DimensionRow> findAllOrderedDimensionRows();

    /**
     * Get a set of dimension row(s) given a set of ApiFilters.
     *
     * @param filters  ApiFilters to use for finding matching dimension rows
     *
     * @return set of dimension row(s)
     *
     * @deprecated  Searching for filtered dimension rows is moving to a paginated version
     * ({@link #findFilteredDimensionRowsPaged})
     * in order to give greater control to the caller.
     */
    @Deprecated
    default TreeSet<DimensionRow> findFilteredDimensionRows(Set<ApiFilter> filters) {
        return new TreeSet<>(
                findFilteredDimensionRowsPaged(filters, PaginationParameters.EVERYTHING_IN_ONE_PAGE).getPageOfData()
        );
    }

    /**
     * Return the desired page of dimension rows that match the specified filters.
     *
     * @param filters  ApiFilters to use for finding matching dimension rows
     * @param paginationParameters  The parameters that define a page (i.e. the number of rows per page,
     * and the page of results desired)
     *
     * @return A page of the dimension rows contained in the Search Provider's indices
     * and matched by the specified filters.
     */
    Pagination<DimensionRow> findFilteredDimensionRowsPaged(
            Set<ApiFilter> filters,
            @NotNull PaginationParameters paginationParameters
    );

    /**
     * Method to add / update indexes.
     *
     * @param rowId  Unique ID for the dimension row
     * @param dimensionRow  New, updated dimension row
     * @param dimensionRowOld  Old dimension row if we're updating one, or null if there is no row for the rowId
     */
    void refreshIndex(String rowId, DimensionRow dimensionRow, DimensionRow dimensionRowOld);

    /**
     * Method to add / update indexes.
     *
     * @param changedRows Collection of newRow / oldRow pairs keyed by rowId to update the index with. Each row is a
     * pair of dimension rows (newRow is the key, oldRow is the value), with the rowId (ie. unique key in the dimension)
     * as the top-level Map key.
     */
    void refreshIndex(Map<String, Pair<DimensionRow, DimensionRow>> changedRows);

    /**
     * Method to check if search provider is healthy.
     *
     * @return true if healthy
     */
    boolean isHealthy();

    /**
     * Clears the dimension cache, and resets the indices, effectively resetting the SearchProvider to a clean state.
     */
    void clearDimension();
}
