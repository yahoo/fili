// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.physicaltables;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.table.BasePhysicalTable;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.availability.BaseMetadataAvailability;

import java.util.Map;

/**
 * A Physical Table that should be backed by a Metadata-based Availability that has only a single data source.
 */
public abstract class SingleDataSourcePhysicalTable extends BasePhysicalTable {

    private final DataSourceName dataSourceName;

    /**
     * Constructor.
     *
     * @param name  Name of the physical table as TableName
     * @param timeGrain  time grain of the table
     * @param columns  The columns for this table
     * @param logicalToPhysicalColumnNames  Mappings from logical to physical names
     * @param availability  Availability that serves interval availability for columns
     */
    public SingleDataSourcePhysicalTable(
            TableName name,
            ZonedTimeGrain timeGrain,
            Iterable<Column> columns,
            Map<String, String> logicalToPhysicalColumnNames,
            BaseMetadataAvailability availability
    ) {
        super(name, timeGrain, columns, logicalToPhysicalColumnNames, availability);
        this.dataSourceName = availability.getDataSourceName();
    }

    public DataSourceName getDataSourceName() {
        return dataSourceName;
    }

    @Override
    public String toString() {
        return String.format("%s datasourceName: %s ", super.toString(), getDataSourceName());
    }
}
