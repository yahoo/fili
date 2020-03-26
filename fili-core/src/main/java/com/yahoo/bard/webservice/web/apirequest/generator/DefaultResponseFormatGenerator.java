// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.ACCEPT_FORMAT_INVALID;

import com.yahoo.bard.webservice.web.BadApiRequestException;
import com.yahoo.bard.webservice.web.DefaultResponseFormatType;
import com.yahoo.bard.webservice.web.ResponseFormatType;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Default generator implementation for binding {@link ResponseFormatType}. Only recognizes response formats from the
 * {@link DefaultResponseFormatType} enum.
 */
public class DefaultResponseFormatGenerator implements Generator<ResponseFormatType> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResponseFormatType.class);

    @Override
    public ResponseFormatType bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return DefaultResponseFormatGenerator.generateResponseFormat(params.getFormat().orElse(null));
    }

    @Override
    public void validate(
            ResponseFormatType entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no default validation
    }

    /**
     * Resolves the {@code format} request parameter into one of the response formats specified in
     * {@link DefaultResponseFormatType}. Defaults to {@link DefaultResponseFormatType#JSON} if {@code format} is null.
     *
     * Throws BadApiRequestException if the response format cannot be resolved to any of the supported response formats
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param format  The string representation of the desired {@link ResponseFormatType} to be parsed. Null values are
     *                accepted and {@link DefaultResponseFormatType#JSON} will be returned if a null value is provided.
     *
     * @return the response format.
     */
    public static ResponseFormatType generateResponseFormat(String format) {
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
