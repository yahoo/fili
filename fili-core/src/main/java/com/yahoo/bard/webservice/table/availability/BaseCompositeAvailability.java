// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class implementing common capabilities for availabilities backed by a collection of other availabilities.
 */
public abstract class BaseCompositeAvailability implements Availability {

    /**
     * Return a stream of all the dependent availabilities.
     *
     * @return A stream of availabilities
     */
    protected abstract Stream<Availability> getAllDependentAvailabilities();

    @Override
    public Set<TableName> getDataSourceNames() {
        return getAllDependentAvailabilities()
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(
                        Collectors.collectingAndThen(
                                Collectors.toSet(),
                                Collections::unmodifiableSet
                        )
                );
    }

    /**
     * Retrieve all available intervals for all columns across all the underlying datasources.
     * <p>
     * Available intervals for the same columns are unioned into a <tt>SimplifiedIntervalList</tt>
     *
     * @return a map of column to all of its available intervals in union
     */
    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return getAllDependentAvailabilities()
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
