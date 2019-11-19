// Copyright 2019, Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

/**
 * Search provider that supports text search queries.
 */
public interface SearchQuerySearchProvider extends SearchProvider {

    /**
     * Given a query string to search against, provides the DimensionRows that are found to be appropriate for that
     * query.
     *
     * @param searchQueryString  The search query to use
     * @param paginationParameters  The parameters that define a page (i.e. the number of rows per page,
     * and the page of results desired)
     *
     * @return the paginated results of the search query
     */
    Pagination<DimensionRow> findSearchRowsPaged(String searchQueryString, PaginationParameters paginationParameters);
}
