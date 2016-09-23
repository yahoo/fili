// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata;

import org.joda.time.DateTime;

/**
 * Represents a druid timeBoundary response.
 */
public class TimeBoundaryResponse {

    private DateTime minTime;
    private DateTime maxTime;

    public DateTime getMinTime() {
        return minTime;
    }

    public void setMinTime(String minTime) {
        this.minTime = new DateTime(minTime);
    }

    public DateTime getMaxTime() {
        return maxTime;
    }

    public void setMaxTime(String maxTime) {
        this.maxTime = new DateTime(maxTime);
    }
}
