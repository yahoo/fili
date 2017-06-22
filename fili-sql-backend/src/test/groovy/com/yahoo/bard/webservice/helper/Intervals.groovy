package com.yahoo.bard.webservice.helper

import org.joda.time.DateTime
import org.joda.time.Interval

class Intervals {

    public static Interval interval(String one, String two) {
        return new Interval(DateTime.parse(one), DateTime.parse(two));
    }
}
