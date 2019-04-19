// Copyright 2019 Verizon Media Group
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.BaseSchema;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableSchema;
import com.yahoo.bard.webservice.table.TableUtils;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.availability.PureUnionAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * PureUnionPhysicalTable uses PureUnionAvailability which returns the union of all dependent availabilities and the
 * union of all datasource names produced by child availabilities.
 */
public class PureUnionPhysicalTable extends BasePhysicalTable {

    private static final Logger LOG = LoggerFactory.getLogger(PureUnionPhysicalTable.class);

    private Set<ConfigPhysicalTable> basePhysicalTables;

    /**
     * Constructor.
     *
     * @param tableName  Name of this table
     * @param basePhysicalTables  Set of physical tables which are unioned together.
     */
    public PureUnionPhysicalTable(
            @NotNull TableName tableName,
            @NotNull Set<ConfigPhysicalTable> basePhysicalTables
    ) {
        super(tableName, buildSchema(basePhysicalTables), buildAvailability(basePhysicalTables));
        this.basePhysicalTables = new HashSet<>(basePhysicalTables); // copy of set
    }

    /**
     * Generated the schema for this table, which is the logical union of the schemas of all sub tables. The
     * physical columns and logical to physical column mapping of all sub schemas are all unioned. However, this
     * table requires all sub tables to be on the same time grain.
     *
     * @param basePhysicalTables A collection of base physical tables contributing to the schema
     *
     * @return the schema of the union table
     */
    protected static PhysicalTableSchema buildSchema(Set<ConfigPhysicalTable> basePhysicalTables) {

        if (basePhysicalTables.isEmpty()) {
            throw new IllegalArgumentException("Cannot build a union of zero tables.");
        }

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
        for (Map.Entry<String, Set<String>> entry : physicalColumnToLogicalName.entrySet()) {
            String key = entry.getKey();
            Set<String> value = entry.getValue();
            value.stream().forEach(logicalName -> {
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
                logicalToPhysicalColumnNames.put(logicalName, key);
            });
        }

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

    /**
     * Generated the schema for this table, which is the logical union of the schemas of all sub tables. The
     * physical columns and logical to physical column mapping of all sub schemas are all unioned. All sub tables to
     * be on the same time grain.
     *
     * @param basePhysicalTables  The base physical tables contributing to the schema
     *
     * @return the schema of the union table
     */
    protected static Availability buildAvailability(Set<ConfigPhysicalTable> basePhysicalTables) {
        return new PureUnionAvailability(
                basePhysicalTables.stream()
                        .map(ConfigPhysicalTable::getAvailability)
                        .collect(Collectors.toSet())
        );
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
        if (physicalColumnNames.isEmpty()) {
            errMsg = String.format(
                    "Could not find any physical columns for logical column name %s from union table composed of: ",
                    logicalName
            );
        } else {
            errMsg = String.format(
                    "Found multiple physical columns names for logical column %s. Physical names found: %s," +
                            "from union table composed of: ",
                    logicalName,
                    physicalColumnNames.stream().reduce((reduction, name) -> reduction + ", " + name)
            );
        }

        errMsg += basePhysicalTables.stream()
                .map(ConfigPhysicalTable::getName)
                .reduce((reduction, name) -> reduction + ", " + name);

        LOG.error(errMsg);
        throw new IllegalStateException(errMsg);
    }
}
