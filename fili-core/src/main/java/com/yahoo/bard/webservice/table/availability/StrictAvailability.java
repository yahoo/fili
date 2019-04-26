// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import com.yahoo.bard.webservice.web.ApiFilter;

import org.joda.time.DateTime;

import java.util.Optional;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An availability that provides column and table available interval services for strict physical tables.
 * <p>
 * This availability uses column intersections to determine it's sigular availability.
 */
public class StrictAvailability extends BaseMetadataAvailability {
    protected final DateTime expectedStartDate, expectedEndDate;

    /**
     * Constructor.
     *
     * @param dataSourceName  The name of the data source associated with this Availability
     * @param metadataService  A service containing the datasource segment data
     */
    public StrictAvailability(
            @NotNull DataSourceName dataSourceName,
            @NotNull DataSourceMetadataService metadataService
    ) {
        this(dataSourceName, metadataService, null, null);
    }

    /**
     * Constructor.
     *
     * @param dataSourceName  The name of the data source associated with this Availability
     * @param metadataService  A service containing the datasource segment data
     * @param expectedStartDate  The expected start date of this availability. Empty indicates no expected start date
     * @param expectedEndDate  The expected end date of this availability. Null indicates no expected end date
     */
    public StrictAvailability(
            @NotNull DataSourceName dataSourceName,
            @NotNull DataSourceMetadataService metadataService,
            DateTime expectedStartDate,
            DateTime expectedEndDate
    ) {
        super(dataSourceName, metadataService);
        this.expectedStartDate = expectedStartDate;
        this.expectedEndDate = expectedEndDate;
    }

    /**
     *
     * Fetch a {@link SimplifiedIntervalList} representing the coalesced available intervals on this availability as
     * filtered by the {@link PhysicalDataSourceConstraint}.
     *
     * @param constraint  <tt>PhysicalDataSourceConstraint</tt> containing
     * {@link com.yahoo.bard.webservice.table.Schema} and {@link ApiFilter}s
     *
     * @return A <tt>SimplifiedIntervalList</tt> of intervals available
     */
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {

        Set<String> requestColumns = constraint.getAllColumnPhysicalNames();
        if (requestColumns.isEmpty()) {
            return getAvailableIntervals();
        }

        // Need to ensure requestColumns is not empty in order to prevent returning null by reduce operation
        return requestColumns.stream()
                .map(physicalName -> getAllAvailableIntervals().getOrDefault(
                        physicalName,
                        new SimplifiedIntervalList()
                ))
                .reduce(SimplifiedIntervalList::intersect)
                .orElse(getAvailableIntervals());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        if (constraint instanceof PhysicalDataSourceConstraint) {
            return getAvailableIntervals((PhysicalDataSourceConstraint) constraint);
        }
        // Strict availability cannot check columns without a PhysicalDataSourceConstraint to bind columns
        return getAvailableIntervals();
    }

    @Override
    public Optional<DateTime> getExpectedStartDate(DataSourceConstraint constraint) {
        return Optional.ofNullable(expectedStartDate);
    }

    @Override
    public Optional<DateTime> getExpectedEndDate(DataSourceConstraint constraint) {
        return Optional.ofNullable(expectedEndDate);
    }

    @Override
    public String toString() {
        return "Concrete " + super.toString();
    }
}
