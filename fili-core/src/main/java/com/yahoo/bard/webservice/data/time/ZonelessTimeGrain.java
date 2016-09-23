// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * An interface to support classes which implement a Time Grain without Zone information.
 * Zoneless time grains are like a Platonic Ideal of granularity.  They are unattached to specific boundaries for
 * purposes of rounding or alignment.
 */
public interface ZonelessTimeGrain extends TimeGrain {

    /**
     * Determines if this dateTime falls on a time grain boundary.
     * A zoneless time grain should use the time zone from the date time being passed in for rounding alignment.
     *
     * @param dateTime  A date time to test against the time grain boundary
     *
     * @return true if this date time, under it's own time zone and calendar, meets a boundary on this time grain
     */
    @Override
    default boolean aligns(DateTime dateTime) {
        return dateTime.equals(roundFloor(dateTime));
    }

    /**
     * Apply a timezone to a time grain.
     *
     * @param dateTimeZone  The time zone to associate with the resulting zone time grain
     *
     * @return A time grain with the selected zone
     */
    default ZonedTimeGrain buildZonedTimeGrain(DateTimeZone dateTimeZone) {
        return new ZonedTimeGrain(this, dateTimeZone);
    }
}
