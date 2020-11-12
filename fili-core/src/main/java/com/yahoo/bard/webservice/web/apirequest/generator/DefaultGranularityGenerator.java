// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.UNKNOWN_GRANULARITY;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.GRANULARITY;
import static com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder.RequestResource.TIMEZONE;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.util.GranularityParseException;
import com.yahoo.bard.webservice.web.apirequest.exceptions.BadApiRequestException;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequest;
import com.yahoo.bard.webservice.web.apirequest.DataApiRequestBuilder;
import com.yahoo.bard.webservice.web.apirequest.RequestParameters;
import com.yahoo.bard.webservice.web.util.BardConfigResources;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import javax.validation.constraints.NotNull;

/**
 * Default generator implementation for binding {@link Granularity}. Granularity is dependent on {@link DateTimeZone}
 * already being bound. Ensure the generator for DateTimeZone is called before attempting to bind granularity with this
 * generator.
 */
public class DefaultGranularityGenerator implements Generator<Granularity> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultGranularityGenerator.class);

    /**
     * Binds request granularity into a {@link Granularity} object.
     *
     * Throws BadApiRequestException if granularity is not present. Granularity is required in all data queries. Also
     * is thrown if an invalid granularity is provided.
     * Throws UnsatisfiedApiRequestConstraintsException if timeZone was not built before this generator runs. Timezone
     * CAN be empty, which implies using the default timezone. A NULL timezone indicates timezone has not been
     * generated yet.
     *
     * @param builder  The builder object representing the in progress {@link DataApiRequest}. Previously constructed
     *        resources are available through this object.
     * @param params  The request parameters sent by the client.
     * @param resources  Resources used to build the request, such as the
     *        {@link com.yahoo.bard.webservice.data.config.ResourceDictionaries}.
     * @return the generated granularity.
     */
    @Override
    public Granularity bind(
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        String granularity = params.getGranularity()
                .orElseThrow(() -> new BadApiRequestException("Granularity is missing from the query."));

        if (!builder.isTimeZoneInitialized()) {
            throw new UnsatisfiedApiRequestConstraintsException(
                    GRANULARITY.getResourceName(),
                    Collections.singleton(TIMEZONE.getResourceName())
            );
        }

        if (builder.getTimeZoneIfInitialized().isPresent()) {
            return generateGranularity(
                    granularity,
                    builder.getTimeZoneIfInitialized().get(),
                    resources.getGranularityParser()
            );
        }

        return generateGranularity(granularity, resources.getGranularityParser());
    }

    @Override
    public void validate(
            Granularity entity,
            DataApiRequestBuilder builder,
            RequestParameters params,
            BardConfigResources resources
    ) {
        // no default validation
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance with time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    public static Granularity generateGranularity(
            @NotNull String granularity,
            @NotNull DateTimeZone dateTimeZone,
            @NotNull GranularityParser granularityParser
    ) throws BadApiRequestException {
        try {
            return granularityParser.parseGranularity(granularity, dateTimeZone);
        } catch (GranularityParseException e) {
            LOG.error(UNKNOWN_GRANULARITY.logFormat(granularity), granularity);
            throw new BadApiRequestException(e.getMessage());
        }
    }

    /**
     * Generate a Granularity instance based on a path element.
     *
     * This method is meant for backwards compatibility. If you do not need to use this method for that reason please
     * prefer using a generator instance instead.
     *
     * @param granularity  A string representation of the granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance without time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    public static Granularity generateGranularity(String granularity, GranularityParser granularityParser)
            throws BadApiRequestException {
        try {
            return granularityParser.parseGranularity(granularity);
        } catch (GranularityParseException e) {
            LOG.error(UNKNOWN_GRANULARITY.logFormat(granularity), granularity);
            throw new BadApiRequestException(e.getMessage(), e);
        }
    }
}
