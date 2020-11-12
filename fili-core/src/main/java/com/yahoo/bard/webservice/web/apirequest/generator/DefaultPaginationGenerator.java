// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.BadPaginationException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;
import com.yahoo.bard.webservice.web.util.PaginationParameters;

import java.util.Optional;

/**
 * Default generator implementation for PaginationParameters. This implementation is meant to be shareable between all
 * basic {@link ApiRequest} implementations.
 */
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
     * <p>Throws {@link BadApiRequestException} if 'perPage' or 'page' is not a positive integer, or if either one is
     * emptystring but not both.
     *
     * <p>This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param perPage  The number of rows per page.
     * @param page  The page to display.
     *
     * @return An Optional wrapping a PaginationParameters if both 'perPage' and 'page' exist. This
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
