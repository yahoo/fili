// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.util.DateTimeFormatterFactory.FULLY_OPTIONAL_DATETIME_FORMATTER;

import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.Optional;

/**
 * Interface offering default implementations for the common components of API request objects.
 */
public interface ApiRequest {
    long SYNCHRONOUS_ASYNC_AFTER_VALUE = Long.MAX_VALUE;
    long ASYNCHRONOUS_ASYNC_AFTER_VALUE = -1;

    String SYNCHRONOUS_REQUEST_FLAG = "never";
    String ASYNCHRONOUS_REQUEST_FLAG = "always";

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
}
