// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Availability that masks the availability of a set of columns, exposing that availability as always avaiable. Any
 * masked columns that are not present in the underlying ability are silently ignored. This class is useful for newly
 * added druid columns that will not be reindexed for earlier segments.
 */
public final class ColumnMaskingAvailability extends AbstractAvailabilityDecorator {

    private final Predicate<Dimension> filter;

    /**
     * Constructor.
     *
     * @param target  The availability to wrap
     * @param maskedColumnNames  Names of api columns to mask. This can be metrics or dimension api names.
     */
    public ColumnMaskingAvailability(
            Availability target,
            Set<String> maskedColumnNames
    ) {
        super(target);
        Set<String> names = Collections.unmodifiableSet(new HashSet<>(maskedColumnNames));
        this.filter = d -> !names.contains(d.getApiName());
    }

    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return getTarget().getDataSourceNames(filterConstraint(constraint));
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return getTarget().getAvailableIntervals(filterConstraint(constraint));
    }

    @Override
    public Optional<DateTime> getExpectedStartDate(DataSourceConstraint constraint) {
        return getTarget().getExpectedStartDate(filterConstraint(constraint));
    }

    @Override
    public Optional<DateTime> getExpectedEndDate(DataSourceConstraint constraint) {
        return getTarget().getExpectedEndDate(filterConstraint(constraint));
    }

    private DataSourceConstraint filterConstraint(DataSourceConstraint constraint) {
        return constraint.withDimensionFilter(filter);
    }
}
