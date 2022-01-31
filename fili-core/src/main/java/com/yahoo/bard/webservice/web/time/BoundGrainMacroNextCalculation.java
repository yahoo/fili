// Copyright 2021 Yahoo Inc.
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
public class BoundGrainMacroNextCalculation extends BoundGrainMacroCalculation {

    public BoundGrainMacroNextCalculation(TimeGrain timeGrain) {
        super(timeGrain);
    }

    @Override
    public DateTime getDateTime(DateTime dateTime, Granularity granularity) {
        return super.getDateTime(dateTime, granularity).plus(timeGrain.getPeriod());
    }
}