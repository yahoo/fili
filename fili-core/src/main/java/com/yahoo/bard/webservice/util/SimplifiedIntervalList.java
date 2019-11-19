// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.fasterxml.jackson.annotation.JsonValue;

import org.apache.commons.collections4.IteratorUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.ReadablePeriod;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * A simplified interval list is a list of intervals, ordered by time, expressed in as few intervals as possible
 * (i.e. adjacent and overlapping intervals are combined into a single interval).
 */
public class SimplifiedIntervalList extends LinkedList<Interval> {

    /**
     * Function to iterate an iterator if it has a next element, otherwise return null.
     */
    protected Function<Iterator<Interval>, Interval> getNextIfAvailable = (it) -> it.hasNext() ? it.next() : null;

    /**
     * Constructor.
     */
    public SimplifiedIntervalList() {
        super();
    }

    /**
     * Simplify then build a list.
     *
     * @param intervals  A collection of intervals
     */
    public SimplifiedIntervalList(Collection<Interval> intervals) {
        super(simplifyIntervals(intervals));
    }

    /**
     * Method to convert SimplifiedIntervalList as regular list to address the deserialization issues.
     *
     * @return List of simplified intervals
     */
    @JsonValue
    public List<Interval> asList() {
        return Collections.unmodifiableList(this);
    }

    /**
     * If the intervals are already simplified, simply copy the list.
     *
     * @param intervals  A simplified list of intervals
     */
    public SimplifiedIntervalList(SimplifiedIntervalList intervals) {
        super(intervals);
    }

    /**
     * Takes one or more lists of intervals, and combines them into a single, sorted list with the minimum number of
     * intervals needed to capture exactly the same instants as the original intervals.
     * <p>
     * If any subintervals of the input collection abut or overlap they will be replaced with a single, combined
     * interval.
     * <p>
     * Examples:
     * <ul>
     * <li>['2014/2017', '2015/2020'] will combine into ['2014/2020']
     * <li>['2015/2016', '2016/2017'] will combine into ['2015/2017]
     * <li>['2015/2016', '2013/2014'] will sort into ['2013/2014', '2015/2016']
     * <li>['2015/2015', '2015/2016', '2012/2013'] will sort and combine to ['2012/2013', '2015/2016']
     * </ul>
     * @param intervals  The collection(s) of intervals being collated
     *
     * @return A single list of sorted intervals simplified to the smallest number of intervals able to describe the
     * duration
     */
    @SafeVarargs
    public static SimplifiedIntervalList simplifyIntervals(Collection<Interval>... intervals) {
        Stream<Interval> allIntervals = Stream.empty();
        for (Collection<Interval> intervalCollection : intervals) {
            allIntervals = Stream.concat(allIntervals, intervalCollection.stream());
        }

        return allIntervals
                .sorted(IntervalStartComparator.INSTANCE::compare)
                .collect(getCollector());
    }

    /**
     * Given a sorted linked list of intervals, add the following interval to the end, merging the incoming interval
     * to any tail intervals which overlap or abut with it.
     * <p>
     * In the case where added intervals are at the end of the list, this is efficient. In the case where they are not,
     * this degrades to an insertion sort.
     *
     * @param interval  The interval to be merged and added to this list
     */
    private void appendWithMerge(Interval interval) {
        // Do not store empty intervals
        if (interval.toDurationMillis() == 0) {
            return;
        }

        if (isEmpty()) {
            addLast(interval);
            return;
        }
        final Interval previous = peekLast();

        // If this interval does not belong at the end, removeLast until it does
        if (interval.getStart().isBefore(previous.getStart())) {
            mergeInner(interval);
            return;
        }

        if (previous.gap(interval) != null) {
            addLast(interval);
            return;
        }
        removeLast();
        Interval newEnd = new Interval(
                Math.min(previous.getStartMillis(), interval.getStartMillis()),
                Math.max(previous.getEndMillis(), interval.getEndMillis())
        );
        addLast(newEnd);
    }

    /**
     * Back elements off the list until the insertion is at the correct endpoint of the list and then merge and append
     * the original contents of the list back in.
     *
     * @param interval  The interval to be merged and added
     */
    private void mergeInner(Interval interval) {
        Interval previous = peekLast();
        LinkedList<Interval> buffer = new LinkedList<>();
        while (previous != null && interval.getStart().isBefore(previous.getStart())) {
            buffer.addFirst(previous);
            removeLast();
            previous = peekLast();
        }
        appendWithMerge(interval);
        buffer.stream().forEach(this::appendWithMerge);
    }

    /**
     * A predicate to scan a Simplified Interval List using an iterator and a test predicate.
     * An iterator over the list is scanned until an interval not fully before the interval under test is found. If
     * no such interval exists, a default value is returned. This predicate can be reused as long as each subsequent
     * call to the test has an equal or later start date.
     */
    public static class SkippingIntervalPredicate implements Predicate<Interval> {

        private final boolean defaultValue;
        private final Iterator<Interval> supply;
        private Interval activeInterval;
        private final BiPredicate<Interval, Interval> testPredicate;

        /**
         * Constructor to build a predicate that applies an arbitrary predicate to not-before intervals from the
         * iterator.
         *
         * @param supplyList  The SimplifiedList of intervals to test the predicate against
         * @param testPredicate The predicate to use when testing an interval against the supply
         * @param defaultValue The value for the test if no comparison interval can be found in the list.
         */
        public SkippingIntervalPredicate(
                SimplifiedIntervalList supplyList,
                BiPredicate<Interval, Interval> testPredicate,
                boolean defaultValue
        ) {
            this.supply = supplyList.iterator();
            this.testPredicate = testPredicate;
            this.defaultValue = defaultValue;

            activeInterval = null;
            if (supply.hasNext()) {
                activeInterval = supply.next();
            }
        }

        /**
         * Skip ahead to the indicated DateTime.
         *
         * @param skipAheadTo  Instant to skip to
         */
        private void skipAhead(@NotNull DateTime skipAheadTo) {
            while (activeInterval != null && activeInterval.isBefore(skipAheadTo)) {
                activeInterval = supply.hasNext() ? supply.next() : null;
            }
        }

        /**
         * Skip through the supply intervals until an active interval matches (that is, one which is at or after the
         * test interval) is located and then test it against the testPredicate.
         * If no comparison interval is found, return a default value.
         *
         * @param testInterval  The interval to test against an active interval
         *
         * @return the result of the test predicate with an active interval otherwise default value
         */
        @Override
        public boolean test(Interval testInterval) {
            skipAhead(testInterval.getStart());
            return (activeInterval != null) ? testPredicate.test(testInterval, activeInterval) : defaultValue;
        }
    }

    /**
     * A predicate for testing whether the test interval is a complete subinterval of part of the supply of intervals.
     */
    public static class IsSubinterval extends SkippingIntervalPredicate {

        /**
         * Filter in intervals from the stream that are fully contained by the supply.
         */
        public static final BiPredicate<Interval, Interval> IS_SUBINTERVAL =
                (test, supplyInterval) -> supplyInterval.contains(test);

        /**
         * Construct a subinterval predicate that closes over a supply of intervals.
         *
         * @param supplyList  The intervals to test an interval is contained by
         */
        public IsSubinterval(SimplifiedIntervalList supplyList) {
            super(supplyList, IS_SUBINTERVAL, false);
        }
    }

    /**
     * Only internal calls to linked list mutators should be used to change the list.
     *
     * @param e  Any element to be added
     *
     * @return nothing, a runtime exception is always thrown
     */
    @Override
    public boolean add(Interval e) {
        throw new IllegalAccessError("Do not use add in Simplified Interval List");
    }

    /**
     * Build a collector which appends overlapping intervals, merging and simplifying as it goes.
     *
     * @return A collector for merging simplified intervals
     */
    public static Collector<Interval, SimplifiedIntervalList, SimplifiedIntervalList> getCollector() {
        return Collector.of(
                SimplifiedIntervalList::new,
                SimplifiedIntervalList::appendWithMerge,
                SimplifiedIntervalList::simplifyIntervals
        );
    }

    /**
     * Return the union of this simplified interval list and the intervals of another.
     *
     * @param that  A simplified list of intervals
     *
     * @return A new simplified list containing all subintervals of both this and that.
     */
    public SimplifiedIntervalList union(SimplifiedIntervalList that) {
        return simplifyIntervals(this, that);
    }

    /**
     * Return the intersection of all subintervals in two interval lists.
     *
     * @param that  A simplified list of intervals
     *
     * @return A new simplified interval list whose intervals are all subintervals of this and that.
     */
    public SimplifiedIntervalList intersect(SimplifiedIntervalList that) {
        Iterator<Interval> theseIntervals = this.iterator();
        Iterator<Interval> thoseIntervals = that.iterator();
        Interval thisCurrent = getNextIfAvailable.apply(theseIntervals);
        Interval thatCurrent = getNextIfAvailable.apply(thoseIntervals);
        List<Interval> collected = new ArrayList<>();

        while (thisCurrent != null && thatCurrent != null) {
            if (thisCurrent.overlaps(thatCurrent)) {
                collected.add(thisCurrent.overlap(thatCurrent));
            }
            if (thisCurrent.isBefore(thatCurrent.getEnd())) {
                thisCurrent = getNextIfAvailable.apply(theseIntervals);
            } else {
                thatCurrent = getNextIfAvailable.apply(thoseIntervals);
            }
        }
        return new SimplifiedIntervalList(collected);
    }

    /**
     * Return the subtracted list of all intervals in this that are not in that.
     *
     * @param that  A simplified list of intervals
     *
     * @return A new simplified interval list whose intervals are all subintervals of this and not that
     */
    public SimplifiedIntervalList subtract(SimplifiedIntervalList that) {
        Iterator<Interval> theseIntervals = this.iterator();
        Interval thisCurrent = getNextIfAvailable.apply(theseIntervals);

        if (thisCurrent == null) {
            return new SimplifiedIntervalList();
        }

        Iterator<Interval> thoseIntervals = that.iterator();

        Interval thatCurrent = getNextIfAvailable.apply(thoseIntervals);
        List<Interval> collected = new ArrayList<>();

        while (thisCurrent != null && thatCurrent != null) {
            if (thisCurrent.isBefore(thatCurrent)) {
                // Non overlapping intervals are simply collected
                collected.add(thisCurrent);
            } else if (thisCurrent.overlaps(thatCurrent)) {
                // Take any part of the source interval that lies before an overlap
                if (thisCurrent.getStart().isBefore(thatCurrent.getStart())) {
                    collected.add(new Interval(thisCurrent.getStart(), thatCurrent.getStart()));
                }
                // Truncate out any overlap from the source interval and continue
                if (!thisCurrent.getEnd().isBefore(thatCurrent.getEnd())) {
                    thisCurrent = new Interval(thatCurrent.getEnd(), thisCurrent.getEnd());
                }
            }
            // Advance to the next interval to consider
            if (thisCurrent.isBefore(thatCurrent.getEnd())) {
                thisCurrent = getNextIfAvailable.apply(theseIntervals);
            } else {
                thatCurrent = getNextIfAvailable.apply(thoseIntervals);
            }
        }
        if (thatCurrent == null) {
            collected.add(thisCurrent);
            while (theseIntervals.hasNext()) {
                collected.add(theseIntervals.next());
            }
        }
        return new SimplifiedIntervalList(collected);
    }

    /**
     * Create an Iterator using IntervalPeriodInterators to all intervals of this list broken up into pieces of size
     * period.
     *
     * @param readablePeriod  The period to chunk subintervals into.
     *
     * @return An iterator which returns period sized subintervals of this interval list.
     */
    public Iterator<Interval> periodIterator(ReadablePeriod readablePeriod) {
        List<Iterator<? extends Interval>> periodIterators = this.stream()
                .map(e -> new IntervalPeriodIterator(readablePeriod, e))
                .collect(Collectors.toList());
        return IteratorUtils.chainedIterator(periodIterators);
    }
}
