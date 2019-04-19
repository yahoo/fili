// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;
import com.yahoo.bard.webservice.table.availability.PermissiveAvailability;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * A sibling of strict physical table, but with permissive availability.
 * <p>
 * This is different from {@link StrictPhysicalTable}. {@link PermissivePhysicalTable} is backed by
 * {@link PermissiveAvailability}. The different Availability affects how available intervals of the table are
 * calculated and returned.
 * <p>
 * For example see {@link PhysicalTable#getAvailableIntervals()}, {@link PhysicalTable#getAllAvailableIntervals()}, and
 * {@link PhysicalTable#getTableAlignment()}.
 */
public class PermissivePhysicalTable extends SingleDataSourcePhysicalTable {

    /**
     * Create a permissive physical table.
     *
     * @param name  Name of the physical table as TableName
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param metadataService  Data source metadata service containing availability data for the table
     */
    public PermissivePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull DataSourceMetadataService metadataService
    ) {
        this(
                name,
                timeGrain,
                columns,
                logicalToPhysicalColumnNames,
                new PermissiveAvailability(DataSourceName.of(name.asName()), metadataService)
        );
    }

    /**
     * Create a permissive physical table.
     *
     * @param name  Name of the physical table as TableName
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  Availability that serves interval availability for columns
     */
    public PermissivePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull PermissiveAvailability availability
    ) {
        super(
                name,
                timeGrain,
                columns,
                logicalToPhysicalColumnNames,
                availability
        );
    }

    @Override
    public String toString() {
        return super.toString() + " datasourceName: " + getDataSourceName();
    }
}
