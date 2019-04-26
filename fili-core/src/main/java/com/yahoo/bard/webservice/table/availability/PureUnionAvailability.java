// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.TableUtils;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Availability that bases it's own availability on the union of all of it's wrapped Availability objects.
 */
public class PureUnionAvailability implements Availability {

    private Set<Availability> baseAvailabilities;

    /**
     * Constructor.
     *
     * @param availabilities  The set of availabilities this class unions to produce it's own availability
     */
    public PureUnionAvailability(Set<Availability> availabilities) {
        baseAvailabilities = availabilities;
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return baseAvailabilities.stream()
                .map(Availability::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return baseAvailabilities.stream()
                .map(availability -> availability.getDataSourceNames(constraint))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return TableUtils.unionMergeAvailabilityIntervals(baseAvailabilities.stream());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return baseAvailabilities.stream()
                .map(Availability::getAvailableIntervals)
                .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::union);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return baseAvailabilities.stream()
                .map(availability -> availability.getAvailableIntervals(constraint))
                .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::union);
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof  PureUnionAvailability)) {
            return false;
        }
        PureUnionAvailability that = (PureUnionAvailability) obj;
        return Objects.equals(baseAvailabilities, that.baseAvailabilities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseAvailabilities);
    }
}
