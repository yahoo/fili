// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.web;

import com.yahoo.fili.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

/**
 * A Class to compute the current DateTime for the current macro.
 */
public class CurrentMacroCalculation implements MacroCalculationStrategies {

    @Override
    public DateTime getDateTime(DateTime dateTime, TimeGrain timeGrain) {
        return timeGrain.roundFloor(dateTime);
    }
}
