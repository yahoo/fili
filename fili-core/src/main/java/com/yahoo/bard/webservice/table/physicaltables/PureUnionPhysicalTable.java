// Copyright 2019 Verizon Media Group
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.BaseSchema;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.ConstrainedTable;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;
import com.yahoo.bard.webservice.table.SchemaConstraintValidator;
import com.yahoo.bard.webservice.table.TableUtils;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.PureUnionAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PureUnionPhysicalTable uses PureUnionAvailability which returns the union of all dependent availabilities and the
 * union of all datasource names produced by child availabilities.
 */
public class PureUnionPhysicalTable implements ConfigPhysicalTable {

    private static final Logger LOG = LoggerFactory.getLogger(PureUnionPhysicalTable.class);

    private TableName tableName;
    private Set<ConfigPhysicalTable> basePhysicalTables;
    private Availability unionedAvailability;
    private PhysicalTableSchema schema;

    /**
     * Constructor.
     *
     * @param tableName  Name of this table
     * @param basePhysicalTables  Set of physical tables which are unioned together.
     */
    public PureUnionPhysicalTable(
            TableName tableName,
            Set<ConfigPhysicalTable> basePhysicalTables
    ) {
        this.tableName = tableName;
        this.basePhysicalTables = new HashSet<>(basePhysicalTables); // copy of set
        this.unionedAvailability = new PureUnionAvailability(basePhysicalTables.stream()
                .map(ConfigPhysicalTable::getAvailability)
                .collect(Collectors.toSet())
        );
        this.schema = buildSchema();
    }

    /**
     * Generated the schema for this table, which is the logical union of the schemas of all sub tables. The
     * physical columns and logical to physical column mapping of all sub schemas are all unioned. However, this
     * table requires all sub tables to be on the same time grain.
     *
     * @return the schema of the union table
     */
    protected PhysicalTableSchema buildSchema() {
        // Generate the list of columns on this unioned table
        Set<Column> columns = basePhysicalTables.stream()
                .map(ConfigPhysicalTable::getSchema)
                .map(BaseSchema::getColumns)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        // Generate the unioned logical to physical name mapping
        Map<String, Set<String>> physicalColumnToLogicalName = new HashMap<>();
        columns.forEach(
                column -> {
                    Set<String> logicalColumnNames = basePhysicalTables.stream()
                            .map(ConfigPhysicalTable::getSchema)
                            .filter(schema -> !schema.getLogicalColumnNames(column.getName())
                                    .iterator()
                                    .next()
                                    .equals(column.getName()))
                            .map(schema -> schema.getLogicalColumnNames(column.getName()))
                            .flatMap(Set::stream)
                            .collect(Collectors.toSet());
                    physicalColumnToLogicalName.put(column.getName(), logicalColumnNames);
                }
        );

        // Generate the unioned logical to physical name mapping
        Map<String, String> logicalToPhysicalColumnNames = new HashMap<>();
        physicalColumnToLogicalName.entrySet().forEach(
                entry -> {
                    entry.getValue().stream().forEach(logicalName -> {
                        if (logicalToPhysicalColumnNames.containsKey(logicalName)) {
                            String msg = String.format(
                                    "Error: when building a pure union table, found a single logical name that" +
                                    " maps to multiple different physical columns. Found on tables %s",
                                    basePhysicalTables.stream()
                                            .map(ConfigPhysicalTable::getName)
                                            .collect(Collectors.joining(", "))
                                );
                            LOG.error(msg);
                            throw new IllegalStateException(msg);
                        }
                        logicalToPhysicalColumnNames.put(logicalName, entry.getKey());
                    });
        });

        // Make sure all tables share the same time grain. if so, use this as the grain
        Set<ZonedTimeGrain> grains = basePhysicalTables.stream()
                .map(ConfigPhysicalTable::getSchema)
                .map(PhysicalTableSchema::getTimeGrain)
                .collect(Collectors.toSet());
        if (grains.size() > 1) {
            String msg = String.format(
                "Error: unioned table contains physical tables with more than one time grain. Tried to union tables %s",
                basePhysicalTables.stream()
                        .map(ConfigPhysicalTable::getName)
                        .collect(Collectors.joining(", "))
            );
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        return new PhysicalTableSchema(grains.iterator().next(), columns, logicalToPhysicalColumnNames);
    }

    @Override
    public Availability getAvailability() {
        return unionedAvailability;
    }

    /**
     * Setter for availability. Required for testing.
     *
     * @param availability  availability to use as new pure union availability
     */
    @Deprecated
    protected void setAvailability(Availability availability) {
        this.unionedAvailability = new PureUnionAvailability(Collections.singleton(availability));
    }

    @Override
    public Map<Column, SimplifiedIntervalList> getAllAvailableIntervals() {
        return TableUtils.unionMergeTableIntervals(basePhysicalTables.stream());
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals() {
        return basePhysicalTables.stream()
                .map(ConfigPhysicalTable::getAvailableIntervals)
                .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::union);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        return basePhysicalTables.stream()
                .map(table -> table.getAvailableIntervals(constraint))
                .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::union);
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return basePhysicalTables.stream()
                .map(ConfigPhysicalTable::getDataSourceNames)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public String getPhysicalColumnName(final String logicalName) {
        Set<String> physicalColumnNames = basePhysicalTables.stream()
                .map(table -> table.getPhysicalColumnName(logicalName))
                .collect(Collectors.toSet());

        if (physicalColumnNames.size() == 1) {
            return physicalColumnNames.iterator().next();
        }

        String errMsg;
        if (physicalColumnNames.size() == 0) {
            errMsg = String.format(
                    "Could not find any physical columns for logical column name %s from union table composed of: ",
                    logicalName
            );
        } else {
            errMsg = String.format(
                    "Found multiple physical columns names for logical column %s. Physical names found: %s," +
                            "from union table composed of: ",
                    logicalName,
                    physicalColumnNames.stream().reduce((so_far, name) -> so_far + ", " + name)
            );
        }

        errMsg += basePhysicalTables.stream()
                .map(ConfigPhysicalTable::getName)
                .reduce((so_far, name) -> so_far + ", " + name);

        LOG.error(errMsg);
        throw new IllegalStateException(errMsg);
    }


    @Override
    public PhysicalTableSchema getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return tableName.asName();
    }

    @Override
    public DateTime getTableAlignment() {
        return getSchema().getTimeGrain().roundFloor(
                IntervalUtils.firstMoment(getAllAvailableIntervals().values()).orElse(new DateTime())
        );
    }

    @Override
    public ConstrainedTable withConstraint(DataSourceConstraint constraint) {
        if (!SchemaConstraintValidator.validateConstraintSchema(constraint, getSchema())) {
            SchemaConstraintValidator.logAndThrowConstraintError(LOG, this, constraint);
        }
        return new ConstrainedTable(this, new PhysicalDataSourceConstraint(constraint, getSchema()));
    }
}
