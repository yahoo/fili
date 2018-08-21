// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension.rfc_dimension;

import com.yahoo.bard.webservice.data.dimension.DimensionRow;
import com.yahoo.bard.webservice.data.dimension.TimeoutException;
import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.ApiFilter;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

/**
 * Provides the capability to return dimension rows by
 */
public interface IndexedDomain extends Domain {

    /**
     * The internal unique name for this domain
     */
    String getName();

    /**
     * Gets the number of distinct dimension rows (assuming the key field is unique) in the index.
     *
     * @return The number of dimension rows for this dimension
     */
    OptionalInt getDimensionCardinality();

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
     * Getter for dimension rows in tree set ordered by the natural ordering of dimension rows.
     *
     * @return tree set of dimension rows the Search Provider has in it's indexes
     *
     * @throws TimeoutException If the search times out.
     */
    TreeSet<DimensionRow> findAllOrderedDimensionRows() throws TimeoutException;

    /**
     * Getter for dimension rows in tree set for consistent order.
     * Throws TimeoutException If the search times out.
     *
     * @param comparator A comparator to define the ordering for a given set of dimension rows.
     *
     * @return tree set of dimension rows the Search Provider has in it's indexes
     *
     * @throws TimeoutException If the search times out.
     */
    default TreeSet<DimensionRow> findAllOrderedDimensionRows(Comparator<DimensionRow> comparator)
            throws TimeoutException

    {
        TreeSet<DimensionRow> result = new TreeSet<>(comparator);
        result.addAll(findAllDimensionRows());
        return result;
    }

    /**
     * Get a set of dimension row(s) given a set of ApiFilters.
     *
     * @param filters  ApiFilters to use for finding matching dimension rows
     *
     * @return set of dimension row(s)
     *
     * ({@link #findFilteredDimensionRowsPaged})
     * in order to give greater control to the caller.
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
     * Method to check if search provider is healthy.
     *
     * @return true if healthy
     */
    boolean isHealthy();
}
