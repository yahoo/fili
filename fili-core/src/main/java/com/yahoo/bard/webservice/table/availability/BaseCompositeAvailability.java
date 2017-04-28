// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.StreamUtils;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class implementing common capabilities for availabilities backed by a collection of other availabilities.
 */
public abstract class BaseCompositeAvailability implements Availability {

    private final Set<Availability> sourceAvailabilities;
    private final Set<DataSourceName> dataSourcesNames;

    /**
     * Constructor.
     *
     * @param availabilityStream  A potentially ordered stream of availabilities which supply this composite view
     */
    protected BaseCompositeAvailability(Stream<Availability> availabilityStream) {
        sourceAvailabilities = StreamUtils.toUnmodifiableSet(availabilityStream);
        dataSourcesNames = StreamUtils.toUnmodifiableSet(
                sourceAvailabilities.stream().map(Availability::getDataSourceNames).flatMap(Set::stream)
        );
    }

    /**
     * Return a stream of all the availabilities which this availability composites from.
     *
     * @return A stream of availabilities
     */
    protected Stream<Availability> getAllSourceAvailabilities() {
        return sourceAvailabilities.stream();
    };

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return dataSourcesNames;
    }

    /**
     * Retrieve all available intervals for all data source fields across all the underlying datasources.
     * <p>
     * Available intervals for the same underlying names are unioned into a <tt>SimplifiedIntervalList</tt>
     *
     * @return a map of metadata field names to all of its available intervals in union
     */
    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return getAllSourceAvailabilities()
                .map(Availability::getAllAvailableIntervals)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (value1, value2) -> SimplifiedIntervalList.simplifyIntervals(value1, value2)
                        )
                );
    }
}
