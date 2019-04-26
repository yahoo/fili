// Copyright 2019 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import com.yahoo.bard.webservice.config.BardFeatureFlag;
import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * This utility class adjusts the DateTime for bindIntervals method of DataApiRequestImpl.
 */
public class IntervalBinders {

    protected static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    /**
     * Adjusts the DateTime for the query interval.
     *
     * @param dateTime  current dateTime
     *
     * @return either adjusted dateTime or the current dateTime as is.
     */
    public static DateTime getAdjustedTime (DateTime dateTime) {
        String adjustedTimezone = SYSTEM_CONFIG.getStringProperty(
                BardFeatureFlag.ADJUSTED_TIME_ZONE.getName(),
                "UTC"
        );
        return dateTime.withZone(DateTimeZone.forID(adjustedTimezone))
                       .withZoneRetainFields(DateTimeZone.UTC);
    }
}
