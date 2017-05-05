// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.Set;

/**
 * Constrained table caches the results of applying a {@link DataSourceConstraint} to a Physical table's availability.
 * Since a constraint is already applied, methods that accept a constraint should be considered deprecated.
 */
public class ConstrainedTable implements PhysicalTable {

    private final DataSourceConstraint constraint;
    private final PhysicalTable sourceTable;
    private final Set<TableName> dataSourceNames;
    private final SimplifiedIntervalList availableIntervals;
    private final Map<Column, SimplifiedIntervalList> allAvailableIntervals;

    /**
     * Constructor.
     *
     * @param table  The table being constrained
     * @param constraint  The constraint being applied
     */
    public ConstrainedTable(ConfigPhysicalTable table, DataSourceConstraint constraint) {
        this.constraint = constraint;
        sourceTable = table;

        Availability sourceAvailability = table.getAvailability();

        PhysicalDataSourceConstraint physicalDataSourceConstraint = new PhysicalDataSourceConstraint(
                constraint,
                table.getSchema()
        );

        availableIntervals = sourceAvailability.getAvailableIntervals(physicalDataSourceConstraint);

        allAvailableIntervals = PhysicalTable.getAllAvailableIntervals(
                sourceAvailability.getAllAvailableIntervals(), getSchema()
        );
        dataSourceNames = sourceAvailability.getDataSourceNames(physicalDataSourceConstraint);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return availableIntervals;
    }

    @Override
    public Map<Column, SimplifiedIntervalList> getAllAvailableIntervals() {
        return allAvailableIntervals;
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    @Override
    public String getName() {
        return sourceTable.getName();
    }

    @Override
    public String getPhysicalColumnName(final String logicalName) {
        return sourceTable.getPhysicalColumnName(logicalName);
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return sourceTable.getSchema();
    }

    @Override
    public TableName getTableName() {
        return sourceTable.getTableName();
    }

    @Override
    public DateTime getTableAlignment() {
        return sourceTable.getTableAlignment();
    }

    @Deprecated
    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        // WARN HERE
        if (constraint == this.constraint) {
            return getAvailableIntervals();
        }
        return sourceTable.getAvailableIntervals(constraint);
    }

    @Deprecated
    @Override
    public Set<TableName> getDataSourceNames(DataSourceConstraint constraint) {
        // WARN HERE
        if (constraint == this.constraint) {
            return dataSourceNames;
        }
        return sourceTable.getDataSourceNames(constraint);
    }

    @Deprecated
    @Override
    public ConstrainedTable withConstraint(final DataSourceConstraint constraint) {
        // WARN HERE
        return sourceTable.withConstraint(constraint);
    }
}
