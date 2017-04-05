// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.util;

import com.yahoo.bard.webservice.config.SystemConfig;
import com.yahoo.bard.webservice.config.SystemConfigProvider;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.Granularity;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.base.AbstractInterval;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Methods and Iterators which support interval slicing alignment and set operations.
 */
public class IntervalUtils {

    public static final SystemConfig SYSTEM_CONFIG = SystemConfigProvider.getInstance();

    public static final String ALIGNMENT_EPOCH_STRING = SYSTEM_CONFIG.getStringProperty(
            SYSTEM_CONFIG.getPackageVariableName("alignment_epoch"),
            "1970-01-01T00:00:00Z"
    );

    public static final DateTime SYSTEM_ALIGNMENT_EPOCH = new DateTime(ALIGNMENT_EPOCH_STRING);

    /**
     * For a given interval, return a stream of the subintervals of it that are shared with at least one interval
     * in the collection of compare intervals.
     *
     * @param interval  The interval being matched
     * @param compareIntervals  The collection of intervals being matched against
     *
     * @return Every pairwise overlap
     */
    public static Stream<Interval> getIntervalOverlaps(Interval interval, Collection<Interval> compareIntervals) {
        return compareIntervals.stream().map(interval::overlap).filter(Objects::nonNull);
    }

    /**
     * Find all the interval overlaps between two collections of intervals.
     * <p>
     * If the left set is null, return the right.  This makes this usable in a reduce function.
     *
     * @param left  The intervals being streamed over
     * @param right  The intervals being tested against
     *
     * @return A set of intervals describing the time common to both sets
     */
    public static Set<Interval> getOverlappingSubintervals(Collection<Interval> left, Collection<Interval> right) {
        if (left == null) {
            return new LinkedHashSet<>(right);
        }
        // Each interval in left is matched against all intervals in the right, collecting overlaps
        return left.stream()
                .flatMap(leftElement -> getIntervalOverlaps(leftElement, right))
                .collect(Collectors.toSet());
    }

    /**
     * Find all the interval overlaps between two collections of intervals.
     * <p>
     * If the left set is null, return the right.  This makes this usable in a reduce function.
     *
     * @param left  The intervals being streamed over
     * @param right  The intervals being tested against
     *
     * @return A set of intervals describing the time common to both sets
     */
    public static Set<Interval> getOverlappingSubintervals(Set<Interval> left, Set<Interval> right) {
        return getOverlappingSubintervals((Collection) left, (Collection) right);
    }

    /**
     * Simplify raw intervals and return a map of intervals (dividing them by the grain) to the ordinal of the interval.
     *
     * @param rawIntervals  A collection of intervals to be split
     * @param grain  The grain to split by.
     *
     * @return a map of simplified intervals split according to the grain to an integer that indicates their ordinal
     */
    public static Map<Interval, AtomicInteger> getSlicedIntervals(
            Collection<Interval> rawIntervals,
            Granularity grain
    ) {
        Iterable<Interval> requestIterable = grain.intervalsIterable(rawIntervals);
        AtomicInteger index = new AtomicInteger(0);
        return StreamSupport
                .stream(requestIterable.spliterator(), false)
                .collect(
                        Collectors.toMap(
                                Function.<Interval>identity(),
                                ignored -> new AtomicInteger(index.getAndIncrement()),
                                (x, y) -> { throw new AssertionError(); },
                                LinkedHashMap::new
                        )
                );
    }

    /**
     * Count the intervals after simplifying raw intervals and splitting by grain.
     *
     * @param rawIntervals  A collection of intervals to be split
     * @param grain  The grain to split by.
     *
     * @return a count of intervals as split by grain
     */
    public static long countSlicedIntervals(Collection<Interval> rawIntervals, Granularity grain) {
        Iterable<Interval> requestIterable = grain.intervalsIterable(rawIntervals);
        return StreamSupport.stream(requestIterable.spliterator(), false)
                .count();
    }

    /**
     * Collect all subintervals from a bucketed collection that are not subintervals of a supply.
     * <p>
     * The bucketed list of intervals are split by grain before being tested as subintervals of the supply list.
     *
     * @param supplyIntervals  The intervals which bucketed intervals are being tested against
     * @param bucketedIntervals  The grain bucketed intervals to collect if not in the supply
     * @param granularity  The grain at which to split the bucketingIntervals
     *
     * @return a simplified list of intervals reflecting the intervals in the fromSet which do not appear in the
     * remove set
     */
    public static SimplifiedIntervalList collectBucketedIntervalsNotInIntervalList(
            SimplifiedIntervalList supplyIntervals,
            SimplifiedIntervalList bucketedIntervals,
            Granularity granularity
    ) {
        // Stream the from intervals, split by grain
        Iterable<Interval> bucketIterable = granularity.intervalsIterable(bucketedIntervals);

        // Not in returns true if any part of the stream interval is not 'covered' by the remove intervals.
        Predicate<Interval> notIn = new SimplifiedIntervalList.IsSubinterval(supplyIntervals).negate();
        return StreamSupport.stream(bucketIterable.spliterator(), false)
                .filter(notIn)
                .collect(SimplifiedIntervalList.getCollector());
    }

    /**
     * Collect all subintervals of an interval list of a grain bucketed size which are subintervals of another supply
     * list of intervals.
     *
     * @param supplyIntervals  The interval collection to match bucketedIntervals against
     * @param bucketedIntervals  The grain bucketed intervals to collect if they overlap the supply
     * @param granularity  Grain at which to split the bucketingIntervals
     *
     * @return a simplified list of subintervals of the bucketedIntervals list
     */
    public static SimplifiedIntervalList collectBucketedIntervalsIntersectingIntervalList(
            SimplifiedIntervalList supplyIntervals,
            SimplifiedIntervalList bucketedIntervals,
            Granularity granularity
    ) {
        // Stream the from intervals, split by grain
        Iterable<Interval> bucketedIterable = granularity.intervalsIterable(bucketedIntervals);

        // Predicate to find buckets which overlap
        Predicate<Interval> isIntersecting =
                new SimplifiedIntervalList.SkippingIntervalPredicate(
                        supplyIntervals,
                        AbstractInterval::overlaps,
                        false
                );

        return StreamSupport.stream(bucketedIterable.spliterator(), false)
                .filter(isIntersecting)
                .collect(SimplifiedIntervalList.getCollector());
    }
    /**
     * Sum the length of the intervals in this collection.
     *
     * @param intervals  A collection of time intervals
     *
     * @return The total duration of all the intervals
     */
    public static long getTotalDuration(Collection<Interval> intervals) {
        return new SimplifiedIntervalList(intervals).stream()
                .map(Interval::toDuration)
                .mapToLong(Duration::getMillis)
                .sum();
    }

    /**
     * Return the first date time instant in this set of intervals if available.
     *
     * @param intervalSets  A collection of intervals sets
     *
     * @return Return the earliest date time (if any) across all the intervals contained
     */
    public static Optional<DateTime> firstMoment(Collection<? extends Collection<Interval>> intervalSets) {
        return intervalSets.stream().flatMap(Collection::stream)
                .map(Interval::getStart)
                .reduce(Utils::getMinValue);
    }

    /**
     * Returns the coarsest ZonedTimeGrain among a set of ZonedTimeGrains.
     * <p>
     * If the set of ZonedTimeGrains is empty, return null.
     *
     * @param zonedTimeGrains  A set of ZonedTimeGrains among which the coarsest ZonedTimeGrain is to be returned.
     *
     * @return the coarsest ZonedTimeGrain among a set of ZonedTimeGrains
     */
    public static ZonedTimeGrain getCoarsestTimeGrain(Collection<ZonedTimeGrain> zonedTimeGrains) {
        return zonedTimeGrains.stream()
                .sorted(Comparator.comparing(ZonedTimeGrain::getEstimatedDuration))
                .findFirst()
                .orElse(null);
    }
}
