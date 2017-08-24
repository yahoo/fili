// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.builders

import org.joda.time.DateTime
import org.joda.time.Interval

class Intervals {

    public static Interval interval(String one, String two) {
        return new Interval(DateTime.parse(one), DateTime.parse(two));
    }
}
