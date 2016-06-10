// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.web.PageNotFoundException;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * Pagination Class
 * @param <T> collection type
 */
public class Pagination<T> {

    // Collection to be paginated
    private final Collection<T> entireCollection;
    private final List<T> pageOfData;

    private final int pageToFetch;
    private final int countPerPage;
    private static final int FIRST_PAGE = 1;
    private final int lastPage;

    /**
     * Constructor
     *
     * @param entireCollection  Collection of entries to be paginated. We guarantee consistent pagination (requesting
     * page 2 of the same collection always returns the same sublist) if and only if the Collection guarantees a
     * consistent iteration order.
     * @param paginationParameters  The parameters needed for pagination.
     * @throws PageNotFoundException if pageToFetch is greater than the number of pages.
     */
    public Pagination(Collection<T> entireCollection, PaginationParameters paginationParameters) {
        this.entireCollection = entireCollection;
        this.pageToFetch = paginationParameters.getPage();
        this.countPerPage = paginationParameters.getPerPage();
        this.lastPage = entireCollection.size() > 0 ? 1 + (entireCollection.size() - 1) / countPerPage : 1;
        if (this.pageToFetch > this.lastPage || this.pageToFetch < FIRST_PAGE) {
            throw new PageNotFoundException(this.pageToFetch, this.countPerPage, lastPage);
        }
        this.pageOfData = buildCurrentPage();
    }

    /**
     * Returns the initial page number
     *
     * @return The initial page
     */
    public int getInitialPage() {
        return FIRST_PAGE;
    }

    /**
     * Returns the final page number
     *
     * @return The final page
     */
    public int getFinalPage() {
        return lastPage;
    }

    /**
     * Gets the number of the first page if this is not the current page
     *
     * @return The first page
     */
    public OptionalInt getFirstPage() {
        return FIRST_PAGE != pageToFetch ? OptionalInt.of(FIRST_PAGE) : OptionalInt.empty();
    }

    /**
     * Gets the number of the last page if this is not the current page
     *
     * @return The last page
     */
    public OptionalInt getLastPage() {
        return lastPage != pageToFetch ? OptionalInt.of(lastPage) : OptionalInt.empty();
    }

    /**
     * Gets the number of the current page
     *
     * @return The current page
     */
    public int getPage() {
        return pageToFetch;
    }

    /**
     * Gets the maximum size of a page
     *
     * @return The maximum page size
     */
    public int getPerPage() {
        return countPerPage;
    }

    /**
     * Gets next page if it exists
     *
     * @return The next page
     */
    public OptionalInt getNextPage() {
        return pageToFetch < lastPage ? OptionalInt.of(pageToFetch + 1) : OptionalInt.empty();
    }

    /**
     * Gets previous page if it exists
     *
     * @return The previous page
     */
    public OptionalInt getPreviousPage() {
        return pageToFetch > FIRST_PAGE ? OptionalInt.of(pageToFetch - 1) : OptionalInt.empty();
    }

    /**
     * Builds a list of paginated results
     *
     * @return A list of paginated results
     */
    protected List<T> buildCurrentPage() {
        return entireCollection.stream().skip((pageToFetch - 1) * countPerPage)
                .limit(countPerPage)
                .collect(Collectors.toList());
    }

    /**
     * Get a list of results corresponding to the current page of data
     *
     * @return The List of paginated results
     */
    public List<T> getPageOfData() {
        return pageOfData;
    }

    /**
     * Get the size of all the data
     *
     * @return The data size
     */
    public int getNumResults() {
        return entireCollection.size();
    }
}
