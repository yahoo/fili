// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.PAGINATION_PAGE_INVALID;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import javax.ws.rs.core.Response;

/**
 * Indicates that a page past the last page of (paginated) results is requested.
 */
public class PageNotFoundException extends RuntimeException {

    private final int page;
    private final int rowsPerPage;
    private final int lastPage;

    /**
     * Constructor.
     *
     * @param page  Page requested
     * @param rowsPerPage  Page size
     * @param lastPage  Terminal page available
     */
    public PageNotFoundException(int page, int rowsPerPage, int lastPage) {
        this.page = page;
        this.rowsPerPage = rowsPerPage;
        this.lastPage = lastPage;
    }

    @Override
    public String getMessage() {
        return PAGINATION_PAGE_INVALID.format(page, rowsPerPage, lastPage);
    }

    public String getLogMessage() {
        return PAGINATION_PAGE_INVALID.logFormat(page, rowsPerPage, lastPage);
    }

    public Response.Status getErrorStatus() {
        return NOT_FOUND;
    }
}
