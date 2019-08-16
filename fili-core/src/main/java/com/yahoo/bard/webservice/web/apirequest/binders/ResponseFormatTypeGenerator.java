// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.ACCEPT_FORMAT_INVALID;

import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseFormatType;

import java.util.Locale;

/**
 * Generates a ResponseFormatType object given a String representation of the desired format.
 */
public interface ResponseFormatTypeGenerator {

    /**
     * Generates the format in which the response data is expected.
     *
     * @param format  Expects a URL format query String.
     *
     * @return Response format type (CSV or JSON).
     * @throws BadApiRequestException if the requested format is not found.
     */
    ResponseFormatType generateAcceptFormat(String format) throws BadApiRequestException;

    /**
     * Default implementation of this interface.
     */
    ResponseFormatTypeGenerator DEFAULT_RESPONSE_FORMAT_TYPE_GENERATOR = format -> {
        try {
            return format == null ?
                    DefaultResponseFormatType.JSON :
                    DefaultResponseFormatType.valueOf(format.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new BadApiRequestException(ACCEPT_FORMAT_INVALID.format(format));
        }
    };
}
