package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Optional;

public class DefaultPaginationGenerator implements Generator<PaginationParameters> {

    @Override
    public PaginationParameters bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generatePaginationParameters(
                params.getPerPage().orElse(""),
                params.getPage().orElse("")
        ).orElse(null);
    }

    @Override
    public void validate(
            PaginationParameters entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no default validation
    }

    /**
     * Builds the paginationParameters object, if the request provides both a perPage and page field.
     *
     * @param perPage  The number of rows per page.
     * @param page  The page to display.
     *
     * @return An Optional wrapping a PaginationParameters if both 'perPage' and 'page' exist. This
     * @throws BadApiRequestException if 'perPage' or 'page' is not a positive integer, or if either one is empty
     * string but not both.
     */
    public static Optional<PaginationParameters> generatePaginationParameters(String perPage, String page) {
        try {
            return "".equals(perPage) && "".equals(page) ?
                    Optional.empty() :
                    Optional.of(new PaginationParameters(perPage, page));
        } catch (BadPaginationException invalidParameters) {
            throw new BadApiRequestException(invalidParameters.getMessage());
        }
    }
}
