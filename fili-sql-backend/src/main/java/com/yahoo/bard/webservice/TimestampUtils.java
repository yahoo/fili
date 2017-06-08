// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by hinterlong on 6/5/17.
 */
public class TimestampUtils {
    private TimestampUtils() {

    }

    public static Timestamp timestampFromString(String e) {
        return timestampFromDateTime(DateTime.parse(e));
    }

    public static Timestamp timestampFromDateTime(DateTime dateTime) {
        return timestampFromMillis(dateTime.getMillis());
    }

    public static Timestamp timestampFromMillis(long millis) {
        // removes the current offset while creating timestamp because the time would get pushed back
        return new Timestamp(millis - TimeZone.getDefault().getOffset(new Date().getTime()));
    }
}
