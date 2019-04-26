// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An availability that limits the wrapped availabilities by the interval specified.
 */
public class TimeFilteredAvailability implements Availability {

    protected Supplier<SimplifiedIntervalList> filterInterval;
    protected Availability target;

    /**
     * Constructor.
     * @param target  The availability to wrap
     * @param filterInterval  The interval that bounds this availability
     */
    public TimeFilteredAvailability(
            @NotNull Availability target,
            @NotNull Supplier<SimplifiedIntervalList> filterInterval
    ) {
        this.target = target;
        this.filterInterval = filterInterval;
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return target.getDataSourceNames();
    }

    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return target.getDataSourceNames(constraint);
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return target.getAllAvailableIntervals().entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().intersect(filterInterval.get())
                )
        );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return target.getAvailableIntervals().intersect(filterInterval.get());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return target.getAvailableIntervals(constraint).intersect(filterInterval.get());
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof  TimeFilteredAvailability)) {
            return false;
        }
        TimeFilteredAvailability that = (TimeFilteredAvailability) obj;
        return Objects.equals(filterInterval.get(), that.filterInterval.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterInterval.get(), target);
    }
}
