// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.time;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

/**
 * A Class to compute the current DateTime with the addition of period for the next macro.
 * If no granularity exists, it takes the value of Current plus 1.
 */
public class NextMacroCalculation extends CurrentMacroCalculation {

    @Override
    public DateTime getDateTime(DateTime dateTime, Granularity granularity) {
        DateTime current = super.getDateTime(dateTime, granularity);
        return granularity instanceof TimeGrain ?
            current.plus(((TimeGrain) granularity).getPeriod())
            : current.plus(1);
    }
}
