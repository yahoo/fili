package com.yahoo.bard.webservice.web.apirequest.building;

import com.yahoo.bard.webservice.data.time.DefaultTimeGrain;
import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.GranularityParser;
import com.yahoo.bard.webservice.util.GranularityParseException;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import org.joda.time.DateTimeZone;

/**
 * This interface is intended to be a temporary bridge while ApiRequest and subclass/interfaces are refactored. Note
 * that if you depend on this interface it will likely be removed in future major version increases.
 */
@Deprecated
public interface GranularityGenerator {

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param dateTimeZone  The time zone to use for this granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance with time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    Granularity generateGranularity(String granularity, DateTimeZone dateTimeZone, GranularityParser granularityParser);

    /**
     * Generate a Granularity instance based on a path element.
     *
     * @param granularity  A string representation of the granularity
     * @param granularityParser  The parser for granularity
     *
     * @return A granularity instance without time zone information
     * @throws BadApiRequestException if the string matches no meaningful granularity
     */
    Granularity generateGranularity(String granularity, GranularityParser granularityParser);

    /**
     * Provides a default implementation of this interface.
     *
     * @return
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
