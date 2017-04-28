// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
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
 * This is different from its parent {@link ConcretePhysicalTable}. {@link PermissiveConcretePhysicalTable} is backed
 * {@link PermissiveAvailability}. The different {@link Availability} affects how available intervals of a table are
 * calculated and returned.
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
                new PermissiveAvailability(DataSourceName.of(name.asName()), dataSourceMetadataService)
        );
    }
}
