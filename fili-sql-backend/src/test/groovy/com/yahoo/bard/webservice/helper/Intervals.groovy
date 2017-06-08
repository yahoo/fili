package com.yahoo.bard.webservice.helper

import org.joda.time.DateTime
import org.joda.time.Interval

/**
 * Created by hinterlong on 6/8/17.
 */
class Intervals {

    public static Interval interval(String one, String two) {
        return new Interval(DateTime.parse(one), DateTime.parse(two));
    }
}
