// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.ReadablePeriod;

import java.util.Objects;

import javax.validation.constraints.NotNull;

/**
 * TimeGrain with a time zone and a Zoneless Time Grain.
 * A key difference from {@link com.yahoo.bard.webservice.data.time.ZonelessTimeGrain} is that alignment testing and
 * rounding on a ZonedTimeGrain will be considered with respect to the ZonedTimeGrain's buckets rather than using the
 * time zone on the date time supplied.
 *
 */
public class ZonedTimeGrain implements TimeGrain {

    private final ZonelessTimeGrain baseTimeGrain;

    private final DateTimeZone timeZone;

    /**
     * Create a Zoned Time grain wrapping a {@link com.yahoo.bard.webservice.data.time.ZonelessTimeGrain} and a joda
     * {@link org.joda.time.DateTimeZone}.
     *
     * @param baseTimeGrain  the unzoned time grain to decorate
     * @param timeZone  A time zone to apply to this time grain
     */
    public ZonedTimeGrain(@NotNull ZonelessTimeGrain baseTimeGrain, @NotNull DateTimeZone timeZone) {
        this.baseTimeGrain = baseTimeGrain;
        this.timeZone = timeZone;
    }

    @Override
    public String getType() {
        return baseTimeGrain.getType();
    }

    @Override
    public String getPeriodString() {
        return baseTimeGrain.getPeriodString();
    }

    /**
     * Use the inner grain's round function to round a {@link org.joda.time.DateTime}, but use the bucketing of this
     * grain's timezone rather than the one from the date time itself.
     *
     * @param dateTime  The time being rounded
     *
     * @return the time, as rounded by the inner time grain and adjusted into this grain's time zone.
     */
    @Override
    public DateTime roundFloor(DateTime dateTime) {
        return baseTimeGrain.roundFloor(dateTime.withZone(timeZone)).withZone(timeZone);
    }

    @Override
    public ReadablePeriod getPeriod() {
        return baseTimeGrain.getPeriod();
    }

    @Override
    public String getName() {
        return baseTimeGrain.getName();
    }

    @Override
    public String getAlignmentDescription() {
        return baseTimeGrain.getAlignmentDescription();
    }

    @Override
    public Duration getEstimatedDuration() {
        return baseTimeGrain.getEstimatedDuration();
    }

    @JsonProperty(value = "timeZone")
    public String getTimeZoneName() {
        return timeZone.toString();
    }

    @Override
    public boolean aligns(DateTime dateTime) {
        return dateTime.withZone(timeZone).equals(roundFloor(dateTime));
    }

    @Override
    public boolean satisfiedBy(TimeGrain grain) {
        if (grain instanceof ZonedTimeGrain) {
            DateTime myBoundary = roundFloor(new DateTime());
            ZonedTimeGrain zonedTimeGrain = (ZonedTimeGrain) grain;
            return baseTimeGrain.satisfiedBy(zonedTimeGrain.baseTimeGrain) && zonedTimeGrain.aligns(myBoundary);
        }
        return baseTimeGrain.satisfiedBy(grain);
    }

    @JsonIgnore
    public ZonelessTimeGrain getBaseTimeGrain() {
        return baseTimeGrain;
    }

    @JsonIgnore
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    /**
     * Create a ZonedTimeGrain with the same base grain and a different time zone.
     *
     * @param dateTimeZone  The time zone to associate with the resulting zone time grain
     *
     * @return The modified copy ZonedTimeGrain
     */
    public ZonedTimeGrain withZone(DateTimeZone dateTimeZone) {
        return new ZonedTimeGrain(this.getBaseTimeGrain(), dateTimeZone);
    }

    @Override
    public String toString() {
        return String.format("%s, zone: %s", baseTimeGrain, timeZone.toString());
    }

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof ZonedTimeGrain)) {
            return false;
        }
        ZonedTimeGrain thatGrain = (ZonedTimeGrain) that;
        return (this.getBaseTimeGrain().equals((thatGrain.getBaseTimeGrain()))) && Objects.equals(
                this.timeZone,
                thatGrain.timeZone
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseTimeGrain, timeZone);
    }
}
