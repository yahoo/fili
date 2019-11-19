// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.time;

import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Iterator;

/**
 * A granularity describes a way of dividing up or organizing time.
 * <p>
 * This is key to the notion of how bard describes time keyed data
 */
public interface Granularity {

    /**
     * Get the name of the granularity.
     *
     * @return the granularity name
     */
    @JsonIgnore
    String getName();

    /**
     * Build an iterator that will iterate over a set of intervals, collecting them into a single stream, and slicing
     * or not slicing them according to the granularity given.
     * <p>
     * IMPORTANT WARNING: The results of this iterator will not always align with the bucketing of query results!
     * <p>
     * Period based granularities will return a single interval per result time bucket from the query.  For the 'all'
     * time grain, multiple intervals are possible, but in query result data they will all map to a single result
     * time bucket.
     *
     * @param intervals  The intervals being iterated across
     *
     * @return A grain delimited iterator returning all the intervals in the interval set
     */
    default Iterator<Interval> intervalsIterator(Collection<Interval> intervals) {
        return intervalsIterator(intervals instanceof SimplifiedIntervalList ?
                (SimplifiedIntervalList) intervals :
                new SimplifiedIntervalList(intervals)
        );
    }

    /**
     * Build an iterator from a pre-simplified list of intervals.
     *
     * @param intervals  The intervals as a simplified interval list
     *
     * @return A grain delimited iterator returning all the intervals in the interval set
     * @see Granularity#intervalsIterator(Collection)
     */
    Iterator<Interval> intervalsIterator(SimplifiedIntervalList intervals);

    /**
     * Wrap the granularity iterator in an iterable predicate for use in streaming.
     *
     * @param intervals  The intervals being iterated across
     *
     * @return An iterable useful for streaming over intervals by grain
     *
     * @see Granularity#intervalsIterator(Collection) (Granularity, Collection)
     */
    default Iterable<Interval> intervalsIterable(Collection<Interval> intervals) {
        return () -> intervalsIterator(intervals);
    }

    /**
     * Determine if this granularity can be fulfilled by an aggregate of another granularity.
     *
     * @param that  The granularity to be compared against
     *
     * @return true if this granularity can be expressed in terms of elements of that granularity
     */
    boolean satisfiedBy(Granularity that);


    /**
     * Determine the reciprocal relationship to satisfiedBy, that this granularity fulfils an aggregate of another
     * granularity.
     *
     * @param that The granularity to be compared against.
     *
     * @return true if that granularity can be expressed in terms of elements of this granularity
     */
    default boolean satisfies(Granularity that) {
        return that.satisfiedBy(this);
    }

    /**
     * Determine if all intervals align with a granularity.
     *
     * @param intervals  The collection of intervals that should be checked
     *
     * @return true if all of the intervals start and end on instants which this granularity bounds
     */
    boolean accepts(Collection<Interval> intervals);

    /**
     * Get description of correct alignment of a granularity, e.g. week
     *
     * @return the correct alignment description
     */
    String getAlignmentDescription();
}
