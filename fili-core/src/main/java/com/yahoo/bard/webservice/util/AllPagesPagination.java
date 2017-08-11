// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.web.PageNotFoundException;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

/**
 * Pagination class that contains all matching results of a query.
 * An instance of AllPagesPagination is responsible for performing pagination, expects
 * the full data set, and perform the trimming itself.
 * This is different from SinglePagePagination which expects an already paginated result set.
 *
 * @param <T> collection type
 */
public class AllPagesPagination<T> implements Pagination<T> {
    // Collection to be paginated
    private final int collectionSize;
    private final List<T> pageOfData;

    private final int pageToFetch;
    private final int countPerPage;
    private final int lastPage;

    /**
     * Constructor.
     *
     * @param entireCollection  Collection of entries to be paginated. We guarantee consistent pagination (requesting
     * page 2 of the same collection always returns the same sublist) if and only if the Collection guarantees a
     * consistent iteration order.
     * @param paginationParameters  The parameters needed for pagination.
     *
     * @throws PageNotFoundException if pageToFetch is greater than the number of pages.
     */
    public AllPagesPagination(Collection<T> entireCollection, PaginationParameters paginationParameters)
            throws PageNotFoundException {
        this.collectionSize = entireCollection.size();
        this.pageToFetch = paginationParameters.getPage(entireCollection.size());
        this.countPerPage = paginationParameters.getPerPage();
        this.lastPage = (collectionSize > countPerPage) ? (collectionSize - 1) / countPerPage + 1 : 1;

        if (this.pageToFetch > this.lastPage || this.pageToFetch < FIRST_PAGE) {
            throw new PageNotFoundException(this.pageToFetch, this.countPerPage, lastPage);
        }

        this.pageOfData = Collections.unmodifiableList(
                entireCollection
                        .stream()
                        .skip((pageToFetch - 1) * countPerPage)
                        .limit(countPerPage)
                        .collect(Collectors.toList())
        );
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
        return collectionSize;
    }
}
