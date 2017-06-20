// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

/**
 * Utility function for creating timestamps.
 */
public class TimestampUtils {
    /**
     * Private constructor - all methods static.
     */
    private TimestampUtils() {

    }

    /**
     * Parses a timestamp from a String.
     *
     * @param time  The time to be parsed.
     *
     * @return the timeStamp created from this time.
     */
    public static Timestamp timestampFromString(String time) {
        return timestampFromDateTime(DateTime.parse(time));
    }

    /**
     * Creates a timestamp from a DateTime.
     *
     * @param dateTime  The dateTime to create the timestamp at.
     *
     * @return the timestamp created from this dateTime.
     */
    public static Timestamp timestampFromDateTime(DateTime dateTime) {
        return timestampFromMillis(dateTime.getMillis());
    }

    /**
     * Creates a timestamp from milliseconds since epoch.
     * NOTE: removes the current timezone offset while creating timestamp.
     *
     * @param millis  The milliseconds of the time to make the timestamp at.
     *
     * @return the timestamp created from this time.
     */
    public static Timestamp timestampFromMillis(long millis) {
        return new Timestamp(millis - TimeZone.getDefault().getOffset(new Date().getTime()));
    }
}
