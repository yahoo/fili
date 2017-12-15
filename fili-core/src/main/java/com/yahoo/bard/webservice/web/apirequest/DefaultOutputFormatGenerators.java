// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.ACCEPT_FORMAT_INVALID;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_ASYNC_AFTER;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Optional;

/**
 * Utility class to hold generator code for response and output formatting.
 */
public class DefaultOutputFormatGenerators {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogicalMetricsGenerators.class);
    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static DefaultOutputFormatGenerators INSTANCE = new DefaultOutputFormatGenerators();

    private static final String SYNCHRONOUS_REQUEST_FLAG = "never";
    private static final String ASYNCHRONOUS_REQUEST_FLAG = "always";

    private static final int DEFAULT_PER_PAGE = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("default_per_page")
    );
    private static final int DEFAULT_PAGE = 1;
    private static final PaginationParameters DEFAULT_PAGINATION = new PaginationParameters(
            DEFAULT_PER_PAGE,
            DEFAULT_PAGE
    );

    /**
     * Builds the paginationParameters object, if the request provides both a perPage and page field.
     *
     * @param perPage  The number of rows per page.
     * @param page  The page to display.
     *
     * @return An Optional wrapping a PaginationParameters if both 'perPage' and 'page' exist.
     * @throws BadApiRequestException if 'perPage' or 'page' is not a positive integer, or if either one is empty
     * string but not both.
     */
    public Optional<PaginationParameters> generatePaginationParameters(String perPage, String page)
            throws BadApiRequestException {
        try {
            return "".equals(perPage) && "".equals(page) ?
                    Optional.empty() :
                    Optional.of(new PaginationParameters(perPage, page));
        } catch (BadPaginationException invalidParameters) {
            throw new BadApiRequestException(invalidParameters.getMessage());
        }
    }

    /**
     * Parses the asyncAfter parameter into a long describing how long the user is willing to wait for the results of a
     * synchronous request before the request should become asynchronous.
     *
     * @param asyncAfterString  asyncAfter should be either a string representation of a long, or the String never
     *
     * @return A long describing how long the user is willing to wait
     *
     * @throws BadApiRequestException if asyncAfterString is neither the string representation of a natural number, nor
     * {@code never}
     */
    public Long generateAsyncAfter(String asyncAfterString) throws BadApiRequestException {
        try {
            if (asyncAfterString == null || "".equals(asyncAfterString)) {
                return null;
            }
            return asyncAfterString.equals(SYNCHRONOUS_REQUEST_FLAG) ?
                    ApiRequest.SYNCHRONOUS_ASYNC_AFTER_VALUE :
                    asyncAfterString.equals(ASYNCHRONOUS_REQUEST_FLAG) ?
                            ApiRequest.ASYNCHRONOUS_ASYNC_AFTER_VALUE :
                            Long.parseLong(asyncAfterString);
        }  catch (NumberFormatException e) {
            LOG.debug(INVALID_ASYNC_AFTER.logFormat(asyncAfterString), e);
            throw new BadApiRequestException(INVALID_ASYNC_AFTER.format(asyncAfterString), e);
        }
    }

    /**
     * Generates the format in which the response data is expected.
     *
     * @param format  Expects a URL format query String.
     *
     * @return Response format type (CSV or JSON).
     * @throws BadApiRequestException if the requested format is not found.
     */
    public ResponseFormatType generateAcceptFormat(String format) throws BadApiRequestException {
        try {
            return format == null ?
                    DefaultResponseFormatType.JSON :
                    DefaultResponseFormatType.valueOf(format.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            LOG.error(ACCEPT_FORMAT_INVALID.logFormat(format), e);
            throw new BadApiRequestException(ACCEPT_FORMAT_INVALID.format(format));
        }
    }
}
