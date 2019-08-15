// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.building;

import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Optional;

/**
 * Generates a PaginationParameters object based on Strings representing the user requested pagination.
 */
public interface PaginationParameterGenerator {

    /**
     * Builds the paginationParameters object, if the request provides both a perPage and page field.
     *
     * @param perPage  The number of rows per page.
     * @param page  The page to display.
     *
     * @return An Optional wrapping a PaginationParameters if both 'perPage' and 'page' exist.
     */
    Optional<PaginationParameters> generatePaginationParameters(String perPage, String page);

    /**
     * Default implementation of this interface.
     */
    PaginationParameterGenerator DEFAULT_PAGINATION_PARAMETER_GENERATOR = (perPage, page) -> {
        try {
            return "".equals(perPage) && "".equals(page) ?
                    Optional.empty() :
                    Optional.of(new PaginationParameters(perPage, page));
        } catch (BadPaginationException invalidParameters) {
            throw new BadApiRequestException(invalidParameters.getMessage());
        }
    };
}
