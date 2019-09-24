// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods to reduce code duplication in table implementations.
 */
public final class TableUtils {

    public static final Function<Stream<Map<?, SimplifiedIntervalList>>, Map<?, SimplifiedIntervalList>>
            ALL_INTERVALS_MERGER = mapStream -> mapStream.map(Map::entrySet)
                    .flatMap(Set::stream)
            .collect(Collectors.toMap(Map.Entry::getKey,
                    Map.Entry::getValue,
                    SimplifiedIntervalList::union,
                    HashMap::new));

    /**
     * Private constructor for utility class.
     */
    private TableUtils() {

    }

    /**
     * Merge all the intervals in a stream of Physical Tables returning a map from column to union of available
     * intervals.
     *
     * @param tableStream  The tables being unioned.
     *
     * @return  A map of columns to intervals.
     */
    @SuppressWarnings(value = "unchecked")
    public static Map<Column, SimplifiedIntervalList> unionMergeTableIntervals(
            Stream<? extends PhysicalTable> tableStream
    ) {
        return (Map<Column, SimplifiedIntervalList>) ALL_INTERVALS_MERGER.apply(
                tableStream.map(PhysicalTable::getAllAvailableIntervals)
        );
    }

    /**
     * Merge all the intervals in a stream of availabilities returning a map from physical column name to union of
     * available intervals.
     *
     * @param availabilityStream  The availabilities being unioned.
     *
     * @return  A map of physical column name to intervals.
     */
    @SuppressWarnings(value = "unchecked")
    public static Map<String, SimplifiedIntervalList> unionMergeAvailabilityIntervals(
            Stream<Availability> availabilityStream
    ) {
        return (Map<String, SimplifiedIntervalList>) ALL_INTERVALS_MERGER.apply(
                availabilityStream.map(Availability::getAllAvailableIntervals)
        );
    }
}
