// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A wrapper around the pagination parameters ('perPage' and 'page') to simplify working with pagination requests.
 * This class is immutable.
 */
public class PaginationParameters {

    private static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final Logger LOG = LoggerFactory.getLogger(PaginationParameters.class);
    private static final int MINIMAL_VALUE = 1;
    private static final int LAST_PAGE = -1;
    private static final String FIRST = "first";
    private static final String LAST = "last";

    private static final int DEFAULT_MAX_RESULTS_WITHOUT_FILTERS = 10000;
    private static final int MAX_RESULTS_WITHOUT_FILTER = SYSTEM_CONFIG.getIntProperty(
            SYSTEM_CONFIG.getPackageVariableName("max_results_without_filters"),
            DEFAULT_MAX_RESULTS_WITHOUT_FILTERS
    );

    /**
     * The common pagination parameter object used by non-paginating method in search providers.
     */
    public static final PaginationParameters EVERYTHING_IN_ONE_PAGE = new PaginationParameters(
            MAX_RESULTS_WITHOUT_FILTER,
            1
    );

    /**
     * Return only the first result.
     */
    public static final PaginationParameters ONE_RESULT = new PaginationParameters(1, 1);

    private final int perPage;
    private final int page;

    /**
     * Given a pair of strings, attempts to parse them into ints representing the pagination parameters.
     *
     * @param perPage  The number of rows on each page of results.
     * @param page  The requested page of data
     *
     * @throws BadPaginationException If at least one of 'perPage' or 'page' is not a positive integer.
     */
    public PaginationParameters(String perPage, String page) throws BadPaginationException {
        this(parseParameter(perPage, "perPage"), parseParameter(page, "page"));
    }

    /**
     * Constructor for already-parsed pagination parameters.
     *
     * @param perPage  The number of rows to be displayed on each page.
     * @param page  The page to be displayed
     */
    public PaginationParameters(int perPage, int page) {
        this.perPage = perPage;
        this.page = page;
    }

    /**
     * Parses the String representation of an integer query parameter.
     *
     * @param parameter  The String to be parsed into an integer.
     * @param parameterName  The name of the parameter to use in the error message.
     *
     * @return the parsed integer
     * @throws BadPaginationException If 'parameter' cannot be parsed as an integer.
     */
    private static int parseParameter (String parameter, String parameterName) throws BadPaginationException {
        if (parameter.equals("")) {
            ErrorMessageFormat errorMessage = ErrorMessageFormat.PAGINATION_PARAMETER_MISSING;
            LOG.debug(errorMessage.logFormat(parameterName));
            throw new BadPaginationException(errorMessage.format(parameterName));
        }
        try {
            if (parameterName.equals("page")) {
                if (parameter.equals(FIRST)) {
                    return 1;
                } else if (parameter.equals(LAST)) {
                    return LAST_PAGE;
                }
            }
            int parsedParameter = Integer.parseInt(parameter);
            validate(parsedParameter, parameterName);
            return parsedParameter;
        } catch (NumberFormatException ignored) {
            ErrorMessageFormat errorMessage = ErrorMessageFormat.PAGINATION_PARAMETER_INVALID;
            LOG.debug(errorMessage.logFormat(parameterName, parameter));
            throw new BadPaginationException(errorMessage.format(parameterName, parameter));
        }
    }

    /**
     * Verifies that the passed parameter is greater than 0.
     *
     * @param parameter  The parameter to be validated.
     * @param parameterName  The name of the parameter to appear in the error message
     *
     * @throws BadPaginationException if 'parameter' is not greater than 0.
     */
    private static void validate(int parameter, String parameterName) throws BadPaginationException {
        if (parameter < MINIMAL_VALUE) {
            ErrorMessageFormat errorMessage = ErrorMessageFormat.PAGINATION_PARAMETER_INVALID;
            LOG.debug(errorMessage.logFormat(parameterName, parameter));
            throw new BadPaginationException(errorMessage.format(parameterName, parameter));
        }
    }

    public int getPerPage() {
        return perPage;
    }

    /**
     * Returns the requested page number.
     *
     * @param resultSize  The number of rows in the result set
     *
     * @return The request page number
     */
    public int getPage(int resultSize) {
        if (page == -1) {
            return resultSize == 0 ? 1 : (int) Math.ceil(((double) resultSize) / perPage);
        }
        return page;
    }

    /**
     * Returns a new PaginationParameters object with the specified page value.
     *
     * @param page  The new page to retrieve.
     *
     * @return A new PaginationParameters object with the same state as this object, except with the specified page.
     *
     * @throws BadPaginationException If page is not a positive Java int.
     */
    public PaginationParameters withPage(String page) throws BadPaginationException {
        return new PaginationParameters(perPage, parseParameter(page, "page"));
    }

    /**
     * Returns a new PaginationParameters object with the specified perPage value.
     *
     * @param perPage  The new number of rows per page.
     *
     * @return A new PaginationParameters object with the same state as this object, except with the specified page.
     *
     * @throws BadPaginationException If page is not a positive Java int.
     */
    public PaginationParameters withPerPage(String perPage) throws BadPaginationException {
        return new PaginationParameters(parseParameter(perPage, "perPage"), page);
    }

    @Override
    public String toString() {
        return "perPage=" + perPage + "&page=" + page;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        PaginationParameters that = (PaginationParameters) o;
        return
                perPage == that.perPage &&
                        page == that.page;
    }

    @Override
    public int hashCode() {
        return Objects.hash(perPage, page);
    }
}
