package com.yahoo.bard.webservice.web.apirequest.building;

import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Optional;

public interface PaginationParameterGenerator {

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
