// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.time;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

/**
 * A Class to compute the current DateTime for the current macro.
 *
 * In the case of the AllGranularity, simply don't round.
 */
public class CurrentMacroCalculation implements MacroCalculationStrategies {


    @Override
    public DateTime getDateTime(DateTime dateTime, Granularity granularity) {
        if (granularity instanceof TimeGrain) {
            return ((TimeGrain) granularity).roundFloor(dateTime);
        }
        return dateTime;
    }
}
