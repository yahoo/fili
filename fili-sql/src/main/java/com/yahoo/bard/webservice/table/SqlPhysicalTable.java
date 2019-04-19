// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.physicaltables.BasePhysicalTable;

import java.util.Map;

/**
 * An implementation of {@link BasePhysicalTable} specific for Sql Backed datasources.
 */
public class SqlPhysicalTable extends BasePhysicalTable {
    private final String schemaName;
    private final String timestampColumn;

    /**
     * Create a physical table.
     *
     * @param name  Fili name of the physical table
     * @param timeGrain  time grain of the table
     * @param columns The columns for this physical table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  The availability of columns in this table
     * @param schemaName  The name of sql schema this table is on.
     * @param timestampColumn  The name of the timestamp column to be used for the database.
     */
    public SqlPhysicalTable(
            TableName name,
            ZonedTimeGrain timeGrain,
            Iterable<Column> columns,
            Map<String, String> logicalToPhysicalColumnNames,
            Availability availability,
            String schemaName,
            String timestampColumn
    ) {
        super(name, timeGrain, columns, logicalToPhysicalColumnNames, availability);
        this.schemaName = schemaName;
        this.timestampColumn = timestampColumn;
    }

    /**
     * Gets the sql schema name this table belongs to.
     *
     * @return the schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Gets the name of the timestamp column backing this table.
     *
     * @return the name of the timestamp column.
     */
    public String getTimestampColumn() {
        return timestampColumn;
    }
}
