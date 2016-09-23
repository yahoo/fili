// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

/**
 * A Class to compute the current DateTime with the addition of period for the next macro.
 */
public class NextMacroCalculation implements MacroCalculationStrategies {

    @Override
    public DateTime getDateTime(DateTime dateTime, TimeGrain timeGrain) {
        return timeGrain.roundFloor(dateTime.plus(timeGrain.getPeriod()));
    }
}
