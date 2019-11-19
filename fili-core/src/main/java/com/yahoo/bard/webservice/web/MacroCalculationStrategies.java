// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

/**
 * Interface implemented by macros to get the dateTime based on granularity.
 */
@FunctionalInterface
public interface MacroCalculationStrategies {

    /**
     * Get the DateTime for the macro given the DateTime to base from and the timeGrain to determine how far to move.
     *
     * @param dateTime  Base instant
     * @param timeGrain  Grain to round to
     *
     * @return the macro-rounded instant
     */
    DateTime getDateTime(DateTime dateTime, TimeGrain timeGrain);
}
