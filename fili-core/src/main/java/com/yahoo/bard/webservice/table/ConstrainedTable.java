// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.QueryPlanningConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Constrained table caches the results of applying a {@link DataSourceConstraint} to a Physical table's availability.
 * Since a constraint is already applied, methods that accept a constraint should be considered deprecated.
 */
public class ConstrainedTable implements PhysicalTable {

    private final DataSourceConstraint constraint;
    private final PhysicalTable sourceTable;
    private final Set<DataSourceName> dataSourceNames;
    private final SimplifiedIntervalList availableIntervals;
    private final Map<Column, SimplifiedIntervalList> allAvailableIntervals;

    /**
     * Constructor.
     *
     * @param sourceTable  The table being constrained
     * @param constraint  The constraint being applied
     */
    public ConstrainedTable(ConfigPhysicalTable sourceTable, DataSourceConstraint constraint) {
        this(sourceTable, new PhysicalDataSourceConstraint(constraint, sourceTable.getSchema()));
    }

    /**
     * Constructor.
     *
     * @param sourceTable  The table being constrained
     * @param constraint  The constraint being applied
     */
    public ConstrainedTable(ConfigPhysicalTable sourceTable, PhysicalDataSourceConstraint constraint) {
        this.constraint = constraint;
        this.sourceTable = sourceTable;

        Availability sourceAvailability = sourceTable.getAvailability();

        availableIntervals = new SimplifiedIntervalList(
                sourceAvailability.getAvailableIntervals(constraint)
        );

        allAvailableIntervals = Collections.unmodifiableMap(
                mapToSchemaAvailability(
                        sourceAvailability.getAllAvailableIntervals(),
                        getSchema()
                )
        );
        dataSourceNames = Collections.unmodifiableSet(
                sourceAvailability.getDataSourceNames(constraint)
        );
    }

    /**
     * Constructor.
     *
     * @param sourceTable  The table being constrained
     * @param queryPlanningConstraint  The constraint being applied
     */
    public ConstrainedTable(ConfigPhysicalTable sourceTable, QueryPlanningConstraint queryPlanningConstraint) {
        this.constraint = queryPlanningConstraint;
        this.sourceTable = sourceTable;

        Availability sourceAvailability = sourceTable.getAvailability();

        PhysicalDataSourceConstraint physicalDataSourceConstraint = new PhysicalDataSourceConstraint(
                constraint,
                getSchema()
        );

        // Physical constraint is necessary to respect column sensitive tables
        availableIntervals =
                sourceAvailability.getAvailableIntervals(physicalDataSourceConstraint);

        allAvailableIntervals = Collections.unmodifiableMap(
                mapToSchemaAvailability(
                        sourceAvailability.getAllAvailableIntervals(),
                        getSchema()
                )
        );
        dataSourceNames = Collections.unmodifiableSet(
                sourceAvailability.getDataSourceNames(constraint)
        );
    }

    private DataSourceConstraint getConstraint() {
        return constraint;
    }

    public PhysicalTable getSourceTable() {
        return sourceTable;
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
    public Set<DataSourceName> getDataSourceNames() {
        return dataSourceNames;
    }

    @Override
    public String getName() {
        return getSourceTable().getName();
    }

    @Override
    public String getPhysicalColumnName(String logicalName) {
        return getSourceTable().getPhysicalColumnName(logicalName);
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return getSourceTable().getSchema();
    }

    @Override
    public TableName getTableName() {
        return getSourceTable().getTableName();
    }

    @Override
    public DateTime getTableAlignment() {
        return getSourceTable().getTableAlignment();
    }

    /**
     * Return a view of the available intervals for the original source table given a constraint.
     *
     * @param constraint  The constraint which limits available intervals
     *
     * @return The intervals that the table can report on
     */
    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        if (getConstraint().equals(constraint)) {
            return getAvailableIntervals();
        }
        return getSourceTable().getAvailableIntervals(constraint);
    }

    /**
     * Return the {@link TableName} of the dataSources which back the original source table given a constraint.
     *
     * @param constraint  A constraint which may narrow the data sources participating.
     *
     * @return A set of tablenames for backing dataSources
     */
    @Override
    public Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        if (getConstraint().equals(constraint)) {
            return getDataSourceNames();
        }
        return getSourceTable().getDataSourceNames(constraint);
    }

    /**
     * Create a constrained copy of the source table.
     *
     * @param constraint  The dataSourceConstraint which narrows the view of the underlying availability
     *
     * @return a constrained table whose availability and serialization are narrowed by this constraint
     */
    @Override
    public ConstrainedTable withConstraint(DataSourceConstraint constraint) {
        return getSourceTable().withConstraint(constraint);
    }
}
