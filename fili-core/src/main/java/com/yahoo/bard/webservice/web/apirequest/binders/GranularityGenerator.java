// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.util.GranularityParseException;
import com.yahoo.bard.webservice.util.Incubating;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import org.joda.time.DateTimeZone;

/**
 * Generator to parse the granularity parameter from an ApiRequest. This interface will likely be removed in favor
 * of just using {@link GranularityParser} directly.
 */
@Incubating
public interface GranularityGenerator {

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance with time zone information
     */
    Granularity generateGranularity(String granularity, DateTimeZone dateTimeZone, GranularityParser granularityParser);

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance without time zone information
     */
    Granularity generateGranularity(String granularity, GranularityParser granularityParser);

    /**
     * A default implementation of this interface. Simply uses the provided granularity parser to parse the provided
     * String representation of the granularity
     */
    GranularityGenerator DEFAULT_GRANULARITY_GENERATOR = new GranularityGenerator() {
            @Override
            public Granularity generateGranularity(
                    String granularity,
                    DateTimeZone dateTimeZone,
                    GranularityParser granularityParser
            ) {
                try {
                    return granularityParser.parseGranularity(granularity, dateTimeZone);
                } catch (GranularityParseException e) {
                    throw new BadApiRequestException(e.getMessage());
                }
            }

            @Override
            public Granularity generateGranularity(String granularity, GranularityParser granularityParser) {
                try {
                    return granularityParser.parseGranularity(granularity);
                } catch (GranularityParseException e) {
                    throw new BadApiRequestException(e.getMessage(), e);
                }
            }
    };
}
