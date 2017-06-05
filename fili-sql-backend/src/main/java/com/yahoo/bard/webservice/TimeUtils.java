package com.yahoo.bard.webservice;

import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by hinterlong on 6/5/17.
 */
public class TimeUtils {
    private TimeUtils() {

    }

    public static Timestamp timestampFromString(String e) {
        return timestampFromDateTime(DateTime.parse(e));
    }

    public static Timestamp timestampFromDateTime(DateTime dateTime) {
        return timestampFromMillis(dateTime.getMillis());
    }

    public static Timestamp timestampFromMillis(long millis) {
        return new Timestamp(millis - TimeZone.getDefault().getOffset(new Date().getTime()));
    }
}
