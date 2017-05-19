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
 * A sibling of concrete physical table, but with permissive availability.
 * <p>
 * This is different from {@link ConcretePhysicalTable}. {@link PermissiveConcretePhysicalTable} is backed by
 * {@link PermissiveAvailability}. The different Availability affects how available intervals of the table are
 * calculated and returned.
 */
public class PermissiveConcretePhysicalTable extends BasePhysicalTable {

    private final DataSourceName dataSourceName;

    /**
     * Create a permissive concrete physical table.
     *
     * @param name  Name of the physical table as TableName
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param metadataService  Data source metadata service containing availability data for the table
     */
    public PermissiveConcretePhysicalTable(
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
     * Create a permissive concrete physical table.
     *
     * @param name  Name of the physical table as TableName
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  Availability that serves interval availability for columns
     */
    public PermissiveConcretePhysicalTable(
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
        this.dataSourceName = availability.getDataSourceName();
    }

    public DataSourceName getDataSourceName() {
        return dataSourceName;
    }

    @Override
    public String toString() {
        return super.toString() + " datasourceName: " + getDataSourceName();
    }
}
