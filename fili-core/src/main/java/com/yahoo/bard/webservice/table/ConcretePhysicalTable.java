// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.ImmutableAvailability;

import java.util.Collections;
import java.util.Map;

import javax.validation.constraints.NotNull;

/**
 * An implementation of Physical table that is backed by a single fact table.
 */
public class ConcretePhysicalTable extends BasePhysicalTable {
    /**
     * Create a concrete physical table.
     * The availability on this table is initialized to empty intervals.
     *
     * @param name  Name of the physical table as TableName
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     */
    public ConcretePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Iterable<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        super(
                name,
                timeGrain,
                columns,
                logicalToPhysicalColumnNames,
                new ImmutableAvailability(name, Collections.emptyMap())
        );
    }

    /**
     * Create a concrete physical table.
     * The fact table name will be defaulted to the name and the availability initialized to empty intervals.
     *
     * @param name  Name of the physical table as String
     * @param timeGrain  time grain of the table
     * @param columns The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     *
     * @deprecated Should use constructor with TableName instead of String as table name
     */
    @Deprecated
    public ConcretePhysicalTable(
            @NotNull String name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Iterable<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames
    ) {
        this(TableName.of(name), timeGrain, columns, logicalToPhysicalColumnNames);
    }

    public String getFactTableName() {
        return getAvailability().getDataSourceNames().stream().findFirst().get().asName();
    }

    @Override
    public String toString() {
        return super.toString() + " factTableName: " + getAvailability().getDataSourceNames() + " alignment: " +
                getTableAlignment();
    }
}
