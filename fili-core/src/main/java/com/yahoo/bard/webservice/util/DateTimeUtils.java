// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Util functions to perform operations on JodaTime objects.
 */
public class DateTimeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtils.class);

    /**
     * Adds timeGrain to a given dateTime.
     *
     * @param dateTime  dateTime to which timeGrain is to be added
     * @param timeGrain  timeGrain to be added
     *
     * @return new dateTime i.e. old dateTime + timeGrain
     */
    public static DateTime addTimeGrain(DateTime dateTime, TimeGrain timeGrain) {
        return dateTime.plus(timeGrain.getPeriod());
    }

    /**
     * Merge all contiguous and overlapping intervals in a set together and return the set with the merged intervals.
     *
     * @param unmergedIntervals A set of intervals that may abut or overlap
     *
     * @return The set of merged intervals
     */
    public static Set<Interval> mergeIntervalSet(Set<Interval> unmergedIntervals) {
        // Create a self sorting set of intervals
        TreeSet<Interval> sortedIntervals = new TreeSet<>(IntervalStartComparator.INSTANCE);

        for (Interval mergingInterval : unmergedIntervals) {
            Iterator<Interval> it = sortedIntervals.iterator();
            while (it.hasNext()) {
                Interval sortedInterval = it.next();
                if (mergingInterval.overlaps(sortedInterval) || mergingInterval.abuts(sortedInterval)) {
                    // Remove the interval being merged with
                    it.remove();
                    // find start and end of new interval
                    DateTime start = (mergingInterval.getStart().isBefore(sortedInterval.getStart())) ?
                            mergingInterval.getStart() : sortedInterval.getStart();
                    DateTime end = (mergingInterval.getEnd().isAfter(sortedInterval.getEnd())) ?
                            mergingInterval.getEnd() : sortedInterval.getEnd();
                    mergingInterval = new Interval(start, end);
                }
            }
            sortedIntervals.add(mergingInterval);
        }
        return sortedIntervals;
    }

    /**
     * Merge an interval into the given interval set.
     *
     * @param intervals  set of intervals to which an interval is to be added/merged
     * @param intervalToMerge  interval to be merged
     *
     * @return set of intervals
     */
    public static Set<Interval> mergeIntervalToSet(Set<Interval> intervals, Interval intervalToMerge) {
        LinkedHashSet<Interval> copyOfOriginalSet = new LinkedHashSet<>(intervals);
        copyOfOriginalSet.add(intervalToMerge);
        return mergeIntervalSet(copyOfOriginalSet);
    }

    /**
     * Finds the gaps in available vs needed interval sets.
     *
     * @param availableIntervals  availability intervals
     * @param neededIntervals  needed intervals
     *
     * @return set of intervals that are needed, but not fully available.
     */
    public static SortedSet<Interval> findFullAvailabilityGaps(
            Set<Interval> availableIntervals,
            Set<Interval> neededIntervals
    ) {
        // Use just one comparator
        Comparator<Interval> intervalStartComparator = new IntervalStartComparator();

        // Sort the intervals by start time, earliest to latest so we iterate over them in order
        SortedSet<Interval> sortedAvailableIntervals = new TreeSet<>(intervalStartComparator);
        sortedAvailableIntervals.addAll(availableIntervals);
        SortedSet<Interval> sortedNeededIntervals = new TreeSet<>(intervalStartComparator);
        sortedNeededIntervals.addAll(neededIntervals);

        // TODO: Consolidate available intervals to remove false misses

        // Get the 1st available interval
        Iterator<Interval> availableIntervalsIterator = sortedAvailableIntervals.iterator();
        if (!availableIntervalsIterator.hasNext()) {
            // We have no available intervals so all needed intervals are missing
            return sortedNeededIntervals;
        }
        Interval available = availableIntervalsIterator.next();

        // Walk through the needed intervals, adding missing ones
        SortedSet<Interval> missingIntervals = new TreeSet<>(intervalStartComparator);
        for (Interval needed : sortedNeededIntervals) {
            // Get the next available interval that can determine availability of the needed interval
            while (!canDetermineAvailability(available, needed) && availableIntervalsIterator.hasNext()) {
                available = availableIntervalsIterator.next();
            }

            // If current available interval contains the needed interval, it's not missing. Next!
            if (available.contains(needed)) {
                continue;
            }

            // Either the needed interval starts before the available interval, or we have no other available intervals.
            missingIntervals.add(needed);
        }

        return missingIntervals;
    }

    /**
     * Check to see if we can determine availability from the given available and needed intervals.
     *
     * @param available  Available interval
     * @param needed  Needed interval
     *
     * @return True if we can determine availability, false if not
     */
    private static boolean canDetermineAvailability(Interval available, Interval needed) {
        if (available != null && needed != null) {
            if (available.contains(needed) || available.getStart().isAfter(needed.getStart())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts an interval to a specified string format.
     *
     * @param interval  interval to be formatted
     * @param formatter  date time formatter for the
     * @param separator  string to separate interval start and end
     *
     * @return formatted interval string
     */
    public static String intervalToString(Interval interval, DateTimeFormatter formatter, String separator) {
        return interval.getStart().toString(formatter)
                + separator
                + interval.getEnd().toString(formatter);
    }

    /**
     * Slices the intervals into smaller intervals of the timeGrain duration.
     *
     * @param interval  interval to be sliced
     * @param timeGrain  size of the slice
     *
     * @return list of intervals obtained by slicing the larger interval
     *
     * @throws java.lang.IllegalArgumentException if the interval is not an even multiple of the time grain
     */
    public static List<Interval> sliceIntervals(Interval interval, TimeGrain timeGrain) {
        // TODO: Refactor me to use a Period
        DateTime intervalEnd = interval.getEnd();
        DateTime sliceStart = interval.getStart();
        DateTime periodStart = timeGrain.roundFloor(sliceStart);

        if (!sliceStart.equals(periodStart)) {
            LOG.info("Interval {} is not aligned to TimeGrain {} starting {}", interval, timeGrain, periodStart);
            throw new IllegalArgumentException("Interval must be aligned to the TimeGrain starting " + periodStart);
        }

        List<Interval> intervalSlices = new ArrayList<>();
        while (sliceStart.isBefore(intervalEnd)) {
            // Find the end of the next slice
            DateTime sliceEnd = DateTimeUtils.addTimeGrain(sliceStart, timeGrain);

            // Make the next slice
            Interval slicedInterval = new Interval(sliceStart, sliceEnd);

            // Make sure that our slice is fully contained within our interval
            if (!interval.contains(slicedInterval)) {
                LOG.info("Interval {} is not a multiple of TimeGrain {}", interval, timeGrain);
                throw new IllegalArgumentException("Interval must be a multiple of the TimeGrain");
            }

            // Add the slice
            intervalSlices.add(slicedInterval);

            // Move the slicer forward
            sliceStart = sliceEnd;
        }
        LOG.debug("Sliced interval {} into {} slices of {} grain", interval, intervalSlices.size(), timeGrain);

        return intervalSlices;
    }


    /**
     * Round the date time back to the beginning of the nearest (inclusive) month of January, April, July, October.
     *
     * @param from  the date being rounded
     *
     * @return  The nearest previous start of month for one of the three quarter months
     */
    public static DateTime quarterlyRound(DateTime from) {
        DateTime.Property property = from.monthOfYear();
        // Shift the month from a one to a zero basis (Jan == 0), then adjust backwards to one of the months that are
        // an integer multiple of three months from the start of the year, then round to the start of that month.
        return property.addToCopy(-1 * ((property.get() - 1) % 3)).monthOfYear().roundFloorCopy();
    }

    /**
     * Given a granularity, produce a time zone.
     *
     * @param granularity  The granularity's time zone, or if there isn't one, the default time zone
     *
     * @return A time zone
     */
    public static DateTimeZone getTimeZone(Granularity granularity) {
        return (granularity instanceof ZonedTimeGrain) ?
                ((ZonedTimeGrain) granularity).getTimeZone() :
                DateTimeZone.getDefault();
    }
}
