// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import java.io.Serializable;
import java.util.List;
import java.util.OptionalInt;

/**
 * Interface for pagination.
 *
 * @param <T> Type of things being paginated
 */
public interface Pagination<T> extends Serializable {

    long serialVersionUID = 1L;
    int FIRST_PAGE = 1;

    /**
     * Gets the number of the current page.
     *
     * @return The current page
     */
    int getPage();

    /**
     * Gets the maximum size of a page.
     *
     * @return The maximum page size
     */
    int getPerPage();

    /**
     * Gets the number of the first page if this is not the current page.
     *
     * @return The first page
     */
    OptionalInt getFirstPage();

    /**
     * Gets the number of the last page if this is not the current page.
     *
     * @return The last page
     */
    OptionalInt getLastPage();

    /**
     * Gets next page if it exists.
     *
     * @return The next page
     */
    OptionalInt getNextPage();

    /**
     * Gets previous page if it exists.
     *
     * @return The previous page
     */
    OptionalInt getPreviousPage();

    /**
     * Get a list of results corresponding to the current page of data.
     *
     * @return The List of paginated results
     */
    List<T> getPageOfData();

    /**
     * Get the size of all the data.
     *
     * @return The data size
     */
    int getNumResults();
}
