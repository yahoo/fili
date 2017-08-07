// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Optional;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * An API Request model object.
 */
public interface ApiRequestInterface {
    /**
     * Get the type of the requested response format.
     *
     * @return The format of the response for this API request.
     */
    ResponseFormatType getFormat();


    /**
     * Get the requested pagination parameters.
     *
     * @return The pagination parameters for this API request
     */
     Optional<PaginationParameters> getPaginationParameters();

    /**
     * Get the uri info.
     *
     * @return The uri info of this API request
     */
     UriInfo getUriInfo();

    /**
     * Get the pagination object associated with this request.
     * This object has valid contents after a call to {@link #getPage}
     *
     * @return The pagination object.
     */
     Pagination<?> getPagination();

    /**
     * Returns how long the user is willing to wait before a request should go asynchronous.
     *
     * @return The maximum number of milliseconds the request is allowed to take before going from synchronous to
     * asynchronous
     */
     long getAsyncAfter();


    /**
     * Get the response builder associated with this request (remove this).
     *
     * @return The response builder.
     */
     Response.ResponseBuilder getBuilder();

    /**
     * Get the default pagination parameters for this type of API request.
     *
     * @return The uri info of this type of API request
     */
     PaginationParameters getDefaultPagination();

}
