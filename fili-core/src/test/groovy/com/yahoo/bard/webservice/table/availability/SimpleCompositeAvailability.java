// Copyright 2019 Verizon Media Group.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Collection;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

/**
 * Simple class extending BaseCompositeAvailability to allow for testing of its methods
 */
class SimpleCompositeAvailability extends BaseCompositeAvailability {

    /**
     * Constructor.
     *
     * @param availabilityStream A potentially ordered stream of availabilities which supply this composite view
     */
    protected SimpleCompositeAvailability(@NotNull Stream<Availability> availabilityStream) {
        super(availabilityStream);
    }

    /**
     * Constructor.
     *
     * @param availabilities A potentially collection of availabilities which supply this composite view
     */
    protected SimpleCompositeAvailability(@NotNull Collection<Availability> availabilities) {
        this(availabilities.stream());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return getAvailableIntervals();
    }
}
