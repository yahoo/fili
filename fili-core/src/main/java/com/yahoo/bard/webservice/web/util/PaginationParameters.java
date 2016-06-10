// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web.util;

import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.ErrorMessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A wrapper around the pagination parameters ('perPage' and 'page') to simplify working with pagination
 * requests. This class is immutable.
 */
public class PaginationParameters {
    private static final Logger LOG = LoggerFactory.getLogger(PaginationParameters.class);
    private static final int MINIMAL_VALUE = 1;

    private final int perPage;
    private final int page;

    /**
     * Given a pair of strings, attempts to parse them into ints representing the pagination parameters.
     *
     * @param perPage  The number of rows on each page of results.
     * @param page  The requested page of data
     * @throws BadPaginationException If at least one of 'perPage' or 'page' is not a positive integer.
     */
    public PaginationParameters(String perPage, String page) {
        this.perPage = parseParameter(perPage, "perPage");
        this.page = parseParameter(page, "page");
        validate(this.perPage, "perPage");
        validate(this.page, "page");
    }


    /**
     * Constructor for already-parsed pagination parameters
     *
     * @param perPage  The number of rows to be displayed on each page.
     * @param page  The page to be displayed
     * @throws BadPaginationException If at least one of 'perPage' or 'page' is not positive.
     */
    public PaginationParameters(int perPage, int page) {
        this.perPage = perPage;
        this.page = page;
        validate(this.perPage, "perPage");
        validate(this.page, "page");
    }

    /**
     * Parses the String representation of an integer query parameter.
     *
     * @param parameter  The String to be parsed into an integer.
     * @param parameterName  The name of the parameter to use in the error message.
     * @throws BadPaginationException If 'parameter' cannot be parsed as an integer.
     */
    private int parseParameter (String parameter, String parameterName) {
        if (parameter.equals("")) {
            ErrorMessageFormat errorMessage = ErrorMessageFormat.PAGINATION_PARAMETER_MISSING;
            LOG.debug(errorMessage.logFormat(parameterName));
            throw new BadPaginationException(errorMessage.format(parameterName));
        }
        try {
            return Integer.parseInt(parameter);
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
     * @throws BadPaginationException if 'parameter' is not greater than 0.
     */
    private void validate(int parameter, String parameterName) {
        if (parameter < MINIMAL_VALUE) {
            ErrorMessageFormat errorMessage = ErrorMessageFormat.PAGINATION_PARAMETER_INVALID;
            LOG.debug(errorMessage.logFormat(parameterName, parameter));
            throw new BadPaginationException(errorMessage.format(parameterName, parameter));
        }
    }

    public int getPerPage() {
        return perPage;
    }

    public int getPage() {
        return page;
    }

    /**
     * Returns a new PaginationParameters object with the specified page value.
     *
     * @param page  The new page to retrieve.
     * @return A new PaginationParameters object with the same state as this object, except with the specified page.
     * @throws BadPaginationException If page is not a positive Java int.
     */
    public PaginationParameters withPage(String page) {
        return new PaginationParameters(perPage, parseParameter(page, "page"));
    }

    /**
     * Returns a new PaginationParameters object with the specified perPage value.
     *
     * @param perPage  The new number of rows per page.
     * @return A new PaginationParameters object with the same state as this object, except with the specified page.
     * @throws BadPaginationException If page is not a positive Java int.
     */
    public PaginationParameters withPerPage(String perPage) {
        return new PaginationParameters(parseParameter(perPage, "perPage"), page);
    }

    @Override
    public boolean equals(Object o) {
        // CHECKSTYLE:OFF
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PaginationParameters that = (PaginationParameters) o;

        if (perPage != that.perPage) return false;
        // CHECKSTYLE:ON
        return page == that.page;

    }

    @Override
    public int hashCode() {
        int result = perPage;
        result = 31 * result + page;
        return result;
    }
}
