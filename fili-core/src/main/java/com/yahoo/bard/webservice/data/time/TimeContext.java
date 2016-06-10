// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.util.GranularityParseException;

import org.joda.time.DateTimeZone;

/**
 * Time Context is a bean container/factory for time grain and granularity objects
 */
public interface TimeContext {

    /**
     * Parse a granularity from a string and time zone.
     *
     * @param granularityName The name of the granularity being parsed
     * @param dateTimeZone  The time zone to associate with the resulting granularity
     *
     * @return The granularity parsed from the name and time zone
     *
     * @throws GranularityParseException if no granularity can be parsed from this name
     */
    Granularity parseGranularity(String granularityName, DateTimeZone dateTimeZone) throws GranularityParseException;

    /**
     * Parse a granularity from a string.
     *
     * @param granularityName The name of the granularity being parsed
     *
     * @return The granularity parsed from the name
     *
     * @throws GranularityParseException if no granularity can be parsed from this granularity
     */
    Granularity parseGranularity(String granularityName) throws GranularityParseException;
}
