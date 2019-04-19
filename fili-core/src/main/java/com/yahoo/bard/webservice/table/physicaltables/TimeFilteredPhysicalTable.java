// Copyright 2019 Verizon Media Group
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.TimeFilteredAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Physical table that wraps another (single) physical and intersects that table's availability with a constant
 * interval to produce it's own availability.
 */
public class TimeFilteredPhysicalTable extends BasePhysicalTable {

    private static final Logger LOG = LoggerFactory.getLogger(TimeFilteredPhysicalTable.class);

    private ConfigPhysicalTable target;
    private Supplier<SimplifiedIntervalList> filterIntervalsSupplier;

    /**
     * Constructor.
     *
     * @param tableName  Name of this table
     * @param target  Wrapped table
     * @param filterIntervalsSupplier  The constant interval which is intersected with the target's availability
     */
    public TimeFilteredPhysicalTable(
            @NotNull TableName tableName,
            @NotNull ConfigPhysicalTable target,
            @NotNull Supplier<SimplifiedIntervalList> filterIntervalsSupplier
    ) {
        super(
                tableName,
                target.getSchema(),
                new TimeFilteredAvailability(target.getAvailability(), filterIntervalsSupplier)
        );
        this.target = target;
        this.filterIntervalsSupplier = filterIntervalsSupplier;
    }

    /**
     * Sets the availability. Required for testing.
     *
     * @param availability  availability to use in new time filtered availability
     */
    @Deprecated
    protected void setAvailability(Availability availability) {
        setAvailability(new TimeFilteredAvailability(availability, filterIntervalsSupplier));
    }

    @Override
    public Map<Column, SimplifiedIntervalList> getAllAvailableIntervals() {
        return target.getAllAvailableIntervals().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().intersect(filterIntervalsSupplier.get())
                ));
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return target.getAvailableIntervals().intersect(filterIntervalsSupplier.get());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return target.getAvailableIntervals(constraint).intersect(filterIntervalsSupplier.get());
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return target.getDataSourceNames();
    }

    @Override
    public String getPhysicalColumnName(final String logicalName) {
        return target.getPhysicalColumnName(logicalName);
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return target.getSchema();
    }

    @Override
    public DateTime getTableAlignment() {
        return target.getTableAlignment();
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof TimeFilteredPhysicalTable)) {
            return false;
        }
        TimeFilteredPhysicalTable that = (TimeFilteredPhysicalTable) obj;
        return super.equals(that) && Objects.equals(target, that.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target) + super.hashCode();
    }
}
