// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER;

import com.yahoo.bard.webservice.util.Pagination;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Interface offering default implementations for the common components of API request objects.
 */
public interface ApiRequest {
    long SYNCHRONOUS_ASYNC_AFTER_VALUE = Long.MAX_VALUE;
    long ASYNCHRONOUS_ASYNC_AFTER_VALUE = -1;

    /**
     * Get the DateTimeFormatter shifted to the given time zone.
     *
     * @param timeZone  TimeZone to shift the default formatter to
     *
     * @return the timezone-shifted formatter
     */
    default DateTimeFormatter generateDateTimeFormatter(DateTimeZone timeZone) {
        return FULLY_OPTIONAL_DATETIME_FORMATTER.withZone(timeZone);
    }

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
     * Get the response builder associated with this request.
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


    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param data  The data to be paginated.
     *
     * @return A stream corresponding to the requested page.
     *
     * @deprecated Pagination is moving to a Stream and pushing creation of the page to a more general
     * method ({@link #getPage(Pagination)}) to allow for more flexibility
     * in how pagination is done.
     */
    @Deprecated
     <T> Stream<T> getPage(Collection<T> data);

    /**
     * Add links to the response builder and return a stream with the requested page from the raw data.
     *
     * @param <T>  The type of the collection elements
     * @param pagination  The pagination object
     *
     * @return A stream corresponding to the requested page.
     */
     <T> Stream<T> getPage(Pagination<T> pagination);
}
