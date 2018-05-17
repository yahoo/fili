// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;

import java.util.Collection;
import java.util.Iterator;

/**
 * TimeGrain represents a strategy for Granularities to map time into monotonic increasing time buckets.
 * Time Grains have a single field joda period. They use estimated duration to create a natural comparison ordering.
 * Time grains support the concept of satisfiability, where a satisfies b if b can always be constructed from one or
 * more a's exactly. Time grains can test an instant to determine whether it 'aligns', i.e. maps to a boundary between
 * mapped time buckets under the grain. Time grains must also implement an error message describing legal alignments
 * when expected alignment fails.
 */
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public interface TimeGrain extends Granularity {

    /**
     * Type of Granularity to use when serializing.
     *
     * @return the type of the TimeGrain
     */
    String getType();

    /**
     * Period string to use when serializing.
     *
     * @return the period string
     */
    @JsonProperty(value = "period")
    String getPeriodString();

    /**
     * Given a date time find the first instant at or before that date time which starts a time grain.
     *
     * @param dateTime  The time being rounded
     *
     * @return The first date time instant of the bucket this date time maps into
     */
    @JsonIgnore
    DateTime roundFloor(DateTime dateTime);

    /**
     * Round the DateTime to the "front edge" (ie. the most recent time) of the time bucket the DateTime falls in.
     *
     * @param dateTime  DateTime to round
     *
     * @return the rounded DateTime
     */
    @JsonIgnore
    default DateTime roundCeiling(DateTime dateTime) {
        DateTime floor = roundFloor(dateTime);
        return floor.equals(dateTime) ? floor : floor.plus(getPeriod());
    }

    @JsonIgnore
    @Override
    String toString();

    /**
     * Get the Period of the TimeGrain.
     *
     * @return the TimeGrain Period
     */
    @JsonIgnore
    ReadablePeriod getPeriod();

    @JsonIgnore
    @Override
    String getName();

    @JsonIgnore
    @Override
    String getAlignmentDescription();

    /**
     * True if the argument grain can be used to build this grain.
     * This grain doesn't need to be a regular multiple of the reference grain. (e.g. months are an irregular number
     * of days long) In the basic case this will support compositions such as days into months or weeks.  If
     * additional narrowing concerns are applied, less specified grains can satisfy more specified grains, but not
     * vice versa.  For example, a day with a time zone can be satisfied by one without a time zone, but zoneless
     * time cannot be satisfied by timezoned time.
     *
     * @param grain  The grain being tested
     *
     * @return true if this grain can be built out of the parameter grain
     */
    boolean satisfiedBy(TimeGrain grain);

    @Override
    default boolean satisfiedBy(Granularity that) {
        return (that instanceof TimeGrain) && this.satisfiedBy((TimeGrain) that);
    }

    /**
     * Estimated duration gives a rough millisecond length of the TimeGrain usable for comparison.
     *
     * @return a millisecond based estimate of the length of this time grain
     */
    @JsonIgnore
    Duration getEstimatedDuration();

    /**
     * Determines if this dateTime falls on a time grain boundary.
     *
     * @param dateTime  A date time to test against the time grain boundary
     *
     * @return true if the date time is at the beginning of a time grain bucket.
     */
    boolean aligns(DateTime dateTime);

    /**
     * Determines if this interval corresponds with time grain boundaries.
     *
     * @param interval  An interval to test against the time grain boundary
     *
     * @return true if the interval starts and stop on a time grain boundaries.
     */
    default boolean aligns(Interval interval) {
        return aligns(interval.getStart()) && aligns(interval.getEnd());
    }

    @Override
    default boolean accepts(Collection<Interval> intervals) {
        return intervals.stream().allMatch(this::aligns);
    }

    @Override
    default Iterator<Interval> intervalsIterator(SimplifiedIntervalList intervals) {
        return intervals.periodIterator(getPeriod());
    }
}
