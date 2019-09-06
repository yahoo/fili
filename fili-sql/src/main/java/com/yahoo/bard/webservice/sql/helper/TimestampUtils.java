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
     * Parses a timestamp of a String.
     *
     * @param time  The time to be parsed.
     *
     * @return the timeStamp created of this time.
     */
    public static Timestamp timestampFromString(String time) {
        return timestampFromDateTime(DateTime.parse(time));
    }

    /**
     * Creates a timestamp of a DateTime.
     *
     * @param dateTime  The dateTime to create the timestamp at.
     *
     * @return the timestamp created of this dateTime.
     */
    public static Timestamp timestampFromDateTime(DateTime dateTime) {
        return Timestamp.valueOf(dateTime.toString("yyyy-MM-dd HH:mm:ss.S"));
    }
}
