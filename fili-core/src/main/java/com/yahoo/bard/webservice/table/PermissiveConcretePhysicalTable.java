// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.availability.PermissiveAvailability;

import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An implementation of concrete physical table with permissive availability.
 * <p>
 * This is different from its parent <tt>ConcretePhysicalTable</tt>. <tt>PermissiveConcretePhysicalTable</tt>
 * is backed <tt>PermissiveAvailability</tt>. As a result, {@link PhysicalTable#getAvailability()} will return
 * the <tt>PermissiveAvailability</tt>. Returning a different <tt>Availability</tt> affects how available intervals
 * of a table are calculated and returned.
 * For example see {@link com.yahoo.bard.webservice.table.BasePhysicalTable#getAvailableIntervals(DataSourceConstraint)}
 * {@link BasePhysicalTable#getAllAvailableIntervals()}, and {@link BasePhysicalTable#getTableAlignment()}.
 */
public class PermissiveConcretePhysicalTable extends ConcretePhysicalTable {
    /**
     * Create a permissive concrete physical table.
     *
     * @param name  Name of the physical table as TableName
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param dataSourceMetadataService  Data source metadata service containing availability data for the table
     */
    public PermissiveConcretePhysicalTable(
            @NotNull TableName name,
            @NotNull ZonedTimeGrain timeGrain,
            @NotNull Set<Column> columns,
            @NotNull Map<String, String> logicalToPhysicalColumnNames,
            @NotNull DataSourceMetadataService dataSourceMetadataService
    ) {
        super(
                name,
                timeGrain,
                columns,
                logicalToPhysicalColumnNames,
                new PermissiveAvailability(name, dataSourceMetadataService)
        );
    }
}
