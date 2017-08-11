// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.web.PageNotFoundException;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.List;
import java.util.OptionalInt;

/**
 * Pagination class that contains only one page of data.
 *
 * @param <T> collection type
 */
public class SinglePagePagination<T> implements Pagination<T> {
    private final List<T> pageOfData;
    private final int pageToFetch;
    private final int countPerPage;
    private final int lastPage;
    private final int totalMatch;

    /**
     * Constructor.
     *
     * @param entirePage  Collection of one page of data
     * @param paginationParameters  The parameters needed for pagination
     * @param totalMatch  The total number of results found. The single page collection is part of these results
     */
    public SinglePagePagination(List<T> entirePage, PaginationParameters paginationParameters, int totalMatch) {
        this.pageToFetch = paginationParameters.getPage(entirePage.size());
        this.countPerPage = paginationParameters.getPerPage();
        this.totalMatch = totalMatch;
        this.lastPage = (totalMatch > countPerPage) ? (totalMatch - 1) / countPerPage + 1 : 1;

        if (this.pageToFetch > this.lastPage || this.pageToFetch < FIRST_PAGE) {
            throw new PageNotFoundException(this.pageToFetch, this.countPerPage, lastPage);
        }
        this.pageOfData = entirePage;
    }

    @Override
    public int getPage() {
        return pageToFetch;
    }

    @Override
    public int getPerPage() {
        return countPerPage;
    }

    @Override
    public OptionalInt getFirstPage() {
        return FIRST_PAGE != pageToFetch ? OptionalInt.of(FIRST_PAGE) : OptionalInt.empty();
    }

    @Override
    public OptionalInt getLastPage() {
        return lastPage != pageToFetch ? OptionalInt.of(lastPage) : OptionalInt.empty();
    }

    @Override
    public OptionalInt getNextPage() {
        return pageToFetch < lastPage ? OptionalInt.of(pageToFetch + 1) : OptionalInt.empty();
    }

    @Override
    public OptionalInt getPreviousPage() {
        return pageToFetch > FIRST_PAGE ? OptionalInt.of(pageToFetch - 1) : OptionalInt.empty();
    }

    @Override
    public List<T> getPageOfData() {
        return pageOfData;
    }

    @Override
    public int getNumResults() {
        return totalMatch;
    }
}
