// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.ConstrainedTable;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;
import com.yahoo.bard.webservice.table.SchemaConstraintValidator;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Base implementation of physical table that are shared across various types of physical tables.
 */
public abstract class BasePhysicalTable implements ConfigPhysicalTable {
    private static final Logger LOG = LoggerFactory.getLogger(BasePhysicalTable.class);

    private final String name;
    private final TableName tableName;
    private final PhysicalTableSchema schema;
    private Availability availability;

    /**
     * Create a physical table.
     *
     * @param name  Fili name of the physical table
     * @param timeGrain  time grain of the table
     * @param columns The columns for this physical table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  The availability of columns in this table
     */
    public BasePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Iterable<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull Availability availability
    ) {
        this.name = name.asName();
        this.tableName = name;
        this.availability = availability;
        this.schema = new PhysicalTableSchema(timeGrain, columns, logicalToPhysicalColumnNames);
    }


    /**
     * Create a physical table.
     *
     * @param name  Fili name of the physical table
     * @param schema  The PhysicalTableSchema desribing this table
     * @param availability  The availability of columns in this table
     */
    public BasePhysicalTable(
            @NotNull TableName name,
            @NotNull PhysicalTableSchema schema,
            @NotNull Availability availability
    ) {
        this.name = name.asName();
        this.tableName = name;
        this.availability = availability;
        this.schema = schema;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        // TODO: Once the availability setter is removed from this class, move this to the constructor
        return getAvailability().getDataSourceNames().stream()
                .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    @Override
    public PhysicalTableSchema getSchema() {
        return schema;
    }

    @Override
    public Availability getAvailability() {
        return availability;
    }

    @Override
    public DateTime getTableAlignment() {
        return getSchema().getTimeGrain().roundFloor(
                IntervalUtils.firstMoment(getAllAvailableIntervals().values()).orElse(new DateTime())
        );
    }

    @Override
    public Map<Column, SimplifiedIntervalList> getAllAvailableIntervals() {
        return mapToSchemaAvailability(
                getAvailability().getAllAvailableIntervals(),
                getSchema()
        );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return getAvailability().getAvailableIntervals();
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        validateConstraintSchema(constraint);
        return getAvailability().getAvailableIntervals(new PhysicalDataSourceConstraint(constraint, getSchema()));
    }

    @Override
    public String getPhysicalColumnName(String logicalName) {
        if (!getSchema().containsLogicalName(logicalName)) {
            LOG.warn(
                    "No mapping found for logical name '{}' to physical name on table '{}'. Will use logical name as " +
                            "physical name. This is unexpected and should not happen for properly configured " +
                            "dimensions.",
                    logicalName,
                    getName()
            );
        }
        return getSchema().getPhysicalColumnName(logicalName);
    }

    /**
     * Used only for testing to inject test availability data into table.
     *
     * @param availability  The test availability for this table
     *
     * @deprecated  Should avoid this method and refine testing strategy to remove this method
     */
    @Deprecated
    protected void setAvailability(Availability availability) {
        this.availability = availability;
    }

    /**
     * Create a constrained copy of this table.
     *
     * @param constraint  The dataSourceConstraint which narrows the view of the underlying availability
     *
     * @return a constrained table whose availability and serialization are narrowed by this constraint

    @Override
    public ConstrainedTable withConstraint(DataSourceConstraint constraint) {
        validateConstraintSchema(constraint);
        return new ConstrainedTable(this, new PhysicalDataSourceConstraint(constraint, getSchema()));
    }*/

    @Override
    public ConstrainedTable withConstraint(DataSourceConstraint constraint) {
        if (!SchemaConstraintValidator.validateConstraintSchema(constraint, getSchema())) {
            SchemaConstraintValidator.logAndThrowConstraintError(LOG, this, constraint);
        }
        return new ConstrainedTable(this, new PhysicalDataSourceConstraint(constraint, getSchema()));
    }


    /**
     * Ensure that the schema of the constraint is consistent with what the table supports.
     *
     * @param constraint  The constraint being tested
     *
     * @throws IllegalArgumentException If there are columns referenced by the constraint unavailable in the table
     */
    private void validateConstraintSchema(DataSourceConstraint constraint) throws IllegalArgumentException {
        Set<String> tableColumnNames = getSchema().getColumnNames();
        // Validate that the requested columns are answerable by the current table
        if (!constraint.getAllColumnNames().stream().allMatch(tableColumnNames::contains)) {
            String message = String.format(
                    "Received invalid request requesting for columns: %s that is not available in this table: %s",
                    constraint.getAllColumnNames().stream()
                            .filter(name -> !tableColumnNames.contains(name))
                            .collect(Collectors.joining(",")), getName()
            );
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof BasePhysicalTable) {
            BasePhysicalTable that = (BasePhysicalTable) obj;
            return Objects.equals(name, that.name)
                    && Objects.equals(schema, that.schema)
                    && Objects.equals(availability, that.availability);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schema, availability);
    }

    @Override
    public String toString() {
        return String.format(
                "Physical table: '%s', schema: '%s', availability: '%s'",
                name,
                schema,
                availability
        );
    }
}
