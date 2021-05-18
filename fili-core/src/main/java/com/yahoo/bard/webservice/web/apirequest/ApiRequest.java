// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER;

import com.yahoo.bard.webservice.util.AllPagesPagination;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/**
 * Interface offering default implementations for the common components of API request objects.
 */
public interface ApiRequest {
    long SYNCHRONOUS_ASYNC_AFTER_VALUE = Long.MAX_VALUE;
    long ASYNCHRONOUS_ASYNC_AFTER_VALUE = -1;

    String SYNCHRONOUS_REQUEST_FLAG = "never";
    String ASYNCHRONOUS_REQUEST_FLAG = "always";

    String COMMA_AFTER_BRACKET_PATTERN = "(?<=]),";

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
     * Get the name of the file for the result to be downloaded as. By default, if the filename is present the response
     * will be returned as an attachment with the return value of this method as its name. If the filename is not
     * present the response may or may not be returned as an attachment.
     * See {@link com.yahoo.bard.webservice.web.util.ResponseUtils} for more details.
     *
     * @return an optional containing the filename of the response attachment.
     */
    default Optional<String> getDownloadFilename() {
        return Optional.empty();
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
     * Returns how long the user is willing to wait before a request should go asynchronous.
     *
     * @return The maximum number of milliseconds the request is allowed to take before going from synchronous to
     * asynchronous
     */
     Long getAsyncAfter();

    /**
     * This method returns a Function that can basically take a Collection and return an instance of
     * AllPagesPagination.
     *
     * @param paginationParameters  The PaginationParameters to be used to generate AllPagesPagination instance
     * @param <T>  The type of items in the Collection which needs to be paginated
     *
     * @return A Function that takes a Collection and returns an instance of AllPagesPagination
     */
    static <T> Function<Collection<T>, AllPagesPagination<T>> getAllPagesPaginationFactory(
            PaginationParameters paginationParameters
    ) {
        return data -> new AllPagesPagination<>(data, paginationParameters);
    }
}
