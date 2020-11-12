// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.INVALID_ASYNC_AFTER;

import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.ApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default generator implementation for async after.
 */
public class DefaultAsyncAfterGenerator implements Generator<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultAsyncAfterGenerator.class);

    @Override
    public Long bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        return generateAsyncAfter(params.getAsyncAfter().orElse(null));
    }

    @Override
    public void validate(
            Long entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no validation
    }

    /**
     * Parses the asyncAfter String representation into a long describing how long the user is willing to wait for the
     * results of a synchronous request before the request should become asynchronous. Null values are replaced with
     * the default async after values. This value is specified by the system config parameter
     * "bard__default_asyncAfter".
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param asyncAfterString  asyncAfter should be null, or a string representation of either a long, or the
     *                          String never
     *
     * @return A long describing how long the user is willing to wait
     *
     * @throws BadApiRequestException if asyncAfterString is neither the string representation of a natural number,
     * nor one of the async after macros: ({@code never, always}).
     */
    public static long generateAsyncAfter(String asyncAfterString) throws BadApiRequestException {
        String async = asyncAfterString;
        if (async == null) {
            async = SystemConfigProvider.getInstance()
                    .getStringProperty(SystemConfigProvider.getInstance().getPackageVariableName("default_asyncAfter"));
        }

        try {
            switch (async) {
                case ApiRequest.SYNCHRONOUS_REQUEST_FLAG:
                    return ApiRequest.SYNCHRONOUS_ASYNC_AFTER_VALUE;
                case ApiRequest.ASYNCHRONOUS_REQUEST_FLAG:
                    return ApiRequest.ASYNCHRONOUS_ASYNC_AFTER_VALUE;
                default:
                   return Long.parseLong(async);
            }
        } catch (NumberFormatException e) {
            LOG.debug(INVALID_ASYNC_AFTER.logFormat(asyncAfterString), e);
            throw new BadApiRequestException(INVALID_ASYNC_AFTER.format(asyncAfterString), e);
        }
    }
}
