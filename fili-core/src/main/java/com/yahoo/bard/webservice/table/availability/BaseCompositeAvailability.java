// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.util.StreamUtils;

import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class implementing common capabilities for availabilities backed by a collection of other availabilities.
 */
public abstract class BaseCompositeAvailability implements Availability {

    protected final Set<Availability> sourceAvailabilities;
    protected final Set<DataSourceName> dataSourcesNames;

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

    @Override
    public Optional<DateTime> getExpectedStartDate(DataSourceConstraint constraint) {
        return getEarliestStart(constraint, sourceAvailabilities);
    }

    @Override
    public Optional<DateTime> getExpectedEndDate(DataSourceConstraint constraint) {
        return getLatestEnd(constraint, sourceAvailabilities);
    }

    /**
     * Finds and returns the earliest expected start date from the provided availabilities. An empty start date
     * is considered to be no start date, and is returned.
     *
     * @param constraint  constraint used to find sub availabilities' expected start dates
     * @param availabilities  the availabilities to examine
     * @return the earliest start date or empty if ANY availability has an empty start date
     */
    protected Optional<DateTime> getEarliestStart(
            DataSourceConstraint constraint,
            Collection<Availability> availabilities
    ) {
        DateTime minDate = availabilities.stream()
                .map(availability -> availability.getExpectedStartDate(constraint).orElse(Availability.DISTANT_PAST))
                .reduce((datetime1, datetime2) -> datetime1.isBefore(datetime2) ? datetime1 : datetime2)
                .orElse(Availability.DISTANT_PAST);
        return minDate.equals(DISTANT_PAST) ? Optional.empty() : Optional.of(minDate);
    }

    /**
     * Finds and returns the latest expected end date from the provided availabilities. An empty end date
     * is considered to be no end date, and is returned.
     *
     * @param constraint  constraint used to find sub availabilities' expected end dates
     * @param availabilities  the availabilities to examine
     * @return the latest end date or empty if ANY availability has an empty end date
     */
    protected Optional<DateTime> getLatestEnd(
            DataSourceConstraint constraint,
            Collection<Availability> availabilities
    ) {
        DateTime maxDate = availabilities.stream()
                .map(availability -> availability.getExpectedEndDate(constraint).orElse(Availability.FAR_FUTURE))
                .reduce((datetime1, datetime2) -> datetime1.isAfter(datetime2) ? datetime1 : datetime2)
                .orElse(Availability.FAR_FUTURE);
        return maxDate.equals(FAR_FUTURE) ? Optional.empty() : Optional.of(maxDate);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof BaseCompositeAvailability)) {
            return false;
        }
        final BaseCompositeAvailability that = (BaseCompositeAvailability) other;
        return Objects.equals(sourceAvailabilities, that.sourceAvailabilities) &&
                Objects.equals(dataSourcesNames, that.dataSourcesNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceAvailabilities, dataSourcesNames);
    }
}
