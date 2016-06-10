// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Weeks;
import org.joda.time.Years;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import javax.validation.constraints.NotNull;

/**
 * DefaultTimeGrain are a set of concrete TimeGrain implementations which support 'natural', simple to describe time
 * blocks corresponding to standard rounding rules.
 * These time grains are all based on single field implementations of Joda's {@link ReadablePeriod}. Satisfiability is
 * determined by configuring an explicit tree together via the satisfying grains property. Rounding functions support
 * alignment testing, and are provided by joda's {@link DateTime.Property} class.
 */
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum DefaultTimeGrain implements ZonelessTimeGrain {
    MINUTE(Minutes.ONE, DateTime::minuteOfDay, " Minute must start and end on the 1st second of a minute."),
    HOUR(Hours.ONE, DateTime::hourOfDay, " Hour must start and end on the 1st minute of an hour.", MINUTE),
    DAY(Days.ONE, DateTime::dayOfYear, " Day must start and end on the 1st hour of a day.", HOUR),
    WEEK(Weeks.ONE, DateTime::weekOfWeekyear, " Week must start on a Monday and end on a Monday.", DAY),
    MONTH(
            Months.ONE,
            DateTime::monthOfYear,
            " Month must start on the 1st of a month and end on the 1st of a month.",
            DAY
    ),
    QUARTER(
            Months.THREE,
            DefaultTimeGrain::quarterlyRound,
            " Quarter must start and end on the 1st day of the 1st, 4th, 7th, or 10th month of a year.",
            MONTH
    ),
    YEAR(Years.ONE, DateTime::year, " Year must start on January 1st and end on January 1st.", QUARTER, MONTH);

    public static final String PERIOD_TYPE_NAME = "period";

    private final ReadablePeriod period;
    private final UnaryOperator<DateTime> roundFloor;
    private final Set<TimeGrain> satisfyingGrains;
    private final Duration estimatedDuration;
    private final String alignmentDescription;

    DefaultTimeGrain(
            ReadablePeriod period,
            Function<DateTime, DateTime.Property> propertyFunction,
            String alignmentDescription,
            TimeGrain... satisfyingGrains
    ) {
        this(
                period,
                (DateTime dateTime) -> propertyFunction.apply(dateTime).roundFloorCopy(),
                alignmentDescription,
                satisfyingGrains
        );
    }

    DefaultTimeGrain(
            ReadablePeriod period,
            UnaryOperator<DateTime> roundFunction,
            String alignmentDescription,
            TimeGrain... satisfyingGrains
    ) {

        this.period = period;
        this.roundFloor = roundFunction;
        this.satisfyingGrains = new HashSet<>(Arrays.asList(satisfyingGrains));
        this.alignmentDescription = alignmentDescription;

        DateTime dateTime = new DateTime();
        this.estimatedDuration = new Duration(roundFloor(dateTime), roundFloor(dateTime).plus(period));
    }

    @Override
    public String getType() {
        return PERIOD_TYPE_NAME;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    @Override
    public ReadablePeriod getPeriod() {
        return period;
    }

    @Override
    public String getAlignmentDescription() {
        return alignmentDescription;
    }

    @Override
    @JsonProperty(value = "period")
    public String getPeriodString() {
        return period.toString();
    }

    @Override
    public DateTime roundFloor(DateTime dateTime) {
        return roundFloor.apply(dateTime);
    }

    @Override
    public boolean satisfiedBy(@NotNull TimeGrain grain) {
        return this == grain || satisfyingGrains.stream().anyMatch(it -> it.satisfiedBy(grain));
    }

    @Override
    public Duration getEstimatedDuration() {
        return estimatedDuration;
    }

    @Override
    public String getName() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    /**
     * Notifies this grain that it can be decomposed to another grain
     *
     * @param satisfies  The grain which this grain can be built from.
     */
    public void addSatisfyingGrain(TimeGrain satisfies) {
        satisfyingGrains.add(satisfies);
    }

    /**
     * Round the date time back to the beginning of the nearest (inclusive) month of January, April, July, October
     *
     * @param from  the date being rounded
     *
     * @return  The nearest previous start of month for one of the three quarter months
     */
    private static DateTime quarterlyRound(DateTime from) {
        DateTime.Property property = from.monthOfYear();
        // Shift the month from a one to a zero basis (Jan == 0), then adjust backwards to one of the months that are
        // an integer multiple of three months from the start of the year, then round to the start of that month.
        return property.addToCopy(-1 * ((property.get() - 1) % 3)).monthOfYear().roundFloorCopy();
    }
}
