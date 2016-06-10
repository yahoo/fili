// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.web;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import org.joda.time.DateTime;

/**
 * Interface implemented by macros to get the dateTime based on granularity.
 */
public interface MacroCalculationStrategies {

    DateTime getDateTime(DateTime dateTime, TimeGrain timeGrain);
}
