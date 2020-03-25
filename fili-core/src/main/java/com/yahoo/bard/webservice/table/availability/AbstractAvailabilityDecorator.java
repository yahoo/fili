// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Abstract decorator implementation of {@link Availability}. This is a utility class intended to help clients implement
 * wrappers over availability. Clients should extend this class when implementing a decorator over Availabilty. This
 * class is abstract to enforce this behavior. This class wraps an {@link Availability} instance and defers all calls to
 * the wrapped instance.
 */
public abstract class AbstractAvailabilityDecorator implements Availability {

    private final Availability target;

    /**
     * Constructor.
     *
     * @param target  The availability to wrap
     */
    public AbstractAvailabilityDecorator(Availability target) {
        this.target = target;
    }

    /**
     * Getter for the wrapped availability.
     *
     * @return the wrapped availability.
     */
    protected Availability getTarget() {
        return target;
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return getTarget().getDataSourceNames();
    }

    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return getTarget().getDataSourceNames(constraint);
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return getTarget().getAllAvailableIntervals();
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return getTarget().getAvailableIntervals();
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return getTarget().getAvailableIntervals(constraint);
    }

    @Override
    public Optional<DateTime> getExpectedStartDate(DataSourceConstraint constraint) {
        return getTarget().getExpectedStartDate(constraint);
    }

    @Override
    public Optional<DateTime> getExpectedEndDate(DataSourceConstraint constraint) {
        return getTarget().getExpectedEndDate(constraint);
    }
}
