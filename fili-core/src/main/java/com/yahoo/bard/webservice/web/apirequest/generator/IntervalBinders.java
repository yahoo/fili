// Copyright 2020 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.generator;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * This utility class adjusts the DateTime for bindIntervals method of DataApiRequestImpl.
 */
public class IntervalBinders {

    protected static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();
    private static final String ADJUSTED_TIME_ZONE_KEY = "adjusted_time_zone";

    /**
     * Adjusts the DateTime for the query interval.
     *
     * @param dateTime  current dateTime
     *
     * @return either adjusted dateTime or the current dateTime as is.
     */
    public static DateTime getAdjustedTime (DateTime dateTime) {
        String adjustedTimeZone = SYSTEM_CONFIG.getStringProperty(SYSTEM_CONFIG.getPackageVariableName(
                ADJUSTED_TIME_ZONE_KEY), "UTC"
        );
        return dateTime.withZone(DateTimeZone.forID(adjustedTimeZone))
                       .withZoneRetainFields(DateTimeZone.UTC);
    }
}
