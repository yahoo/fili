// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
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
     * Gets the number of distinct dimension rows (assuming the key field is unique) in the index.
     *
     * @param  refresh If true, clear any cached cardinality and recalculate before returning.
     *
     * @return The number of dimension rows for this dimension
     */
    default int getDimensionCardinality(boolean refresh) {
        // If no caching is done, simply delegate
        return getDimensionCardinality();
    }

    /**
     * Getter for dimension rows.
     *
     * @return Set of dimension rows the Search Provider has in it's indexes
     *
     */
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
     * Throws TimeoutException If the search times out.
     */
    Pagination<DimensionRow> findAllDimensionRowsPaged(@NotNull PaginationParameters paginationParameters);

    /**
     * Getter for dimension rows in tree set for consistent order.
     * Throws TimeoutException If the search times out.
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
     */
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
     * Throws TimeoutException If the search times out.
     *
     * @return A page of the dimension rows contained in the Search Provider's indices
     * and matched by the specified filters.
     */
    Pagination<DimensionRow> findFilteredDimensionRowsPaged(
            Set<ApiFilter> filters,
            @NotNull PaginationParameters paginationParameters
    );

    /**
     * Determine if any rows match these filters.
     *
     * @param filters  ApiFilters to use for finding matching dimension rows
     *
     * @return true if at least one row is returned.
     */
    default boolean hasAnyRows(Set<ApiFilter> filters) {
        return findFilteredDimensionRowsPaged(filters, PaginationParameters.ONE_RESULT).getNumResults() > 0;
    }

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
     * Replaces index with a new index in hot-swap manner.
     *
     * @param newLuceneIndexPathString  The location of directory that contains the the new index.
     */
    default void replaceIndex(String newLuceneIndexPathString) {
        String message = String.format(
                "Current implementation of SearchProvider: %s does not support index replacement operation.",
                this.getClass().getSimpleName()
        );
        LoggerFactory.getLogger(SearchProvider.class).error(message);
        throw new UnsupportedOperationException(message);
    }

    /**
     * Clears the dimension cache, and resets the indices, effectively resetting the SearchProvider to a clean state.
     */
    void clearDimension();
}
