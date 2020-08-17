// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper;

import org.joda.time.DateTime;

import java.sql.Timestamp;

/**
 * Utility function for creating timestamps.
 */
public class TimestampUtils {
    /**
     * Private constructor - all methods static.
     */
    private TimestampUtils() {
        throw new IllegalStateException("No instances");
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
        return Timestamp.valueOf(dateTime.toString("yyyy-MM-dd HH:mm:ss.S"));
    }

    /**
     * Convert a DateTime to a string with specified format.
     * @param dateTime the Datetime object
     * @param format the format string
     * @return the dateTime string
     */
    public static String dateTimeToString(DateTime dateTime, String format) {
        return dateTime.toString(format);
    }
}
