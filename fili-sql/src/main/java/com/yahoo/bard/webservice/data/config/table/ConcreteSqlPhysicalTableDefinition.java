// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.SqlPhysicalTable;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.availability.PermissiveAvailability;

import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link ConcretePhysicalTableDefinition} specific to SQL backed datasources.
 */
public class ConcreteSqlPhysicalTableDefinition extends ConcretePhysicalTableDefinition {
    private final String schemaName;
    private final String timestampColumn;

    /**
     * Define a sql backed physical table using a zoned time grain. Requires the schema and timestamp column to be
     * specified.
     *
     * @param schemaName  The name of sql schema this table is on.
     * @param timestampColumn  The name of the timestamp column to be used for the database.
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     */
    public ConcreteSqlPhysicalTableDefinition(
            String schemaName,
            String timestampColumn,
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs);
        this.schemaName = schemaName;
        this.timestampColumn = timestampColumn;
    }

    /**
     * Define a physical table with provided logical to physical column name mappings. Requires the schema and timestamp
     * column to be specified.
     *
     * @param schemaName  The name of sql schema this table is on.
     * @param timestampColumn  The name of the timestamp column to be used for the database.
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     * @param logicalToPhysicalNames  A map from logical column names to physical column names
     */
    public ConcreteSqlPhysicalTableDefinition(
            String schemaName,
            String timestampColumn,
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<String, String> logicalToPhysicalNames
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs, logicalToPhysicalNames);
        this.schemaName = schemaName;
        this.timestampColumn = timestampColumn;
    }

    @Override
    public ConfigPhysicalTable build(ResourceDictionaries dictionaries, DataSourceMetadataService metadataService) {
        return new SqlPhysicalTable(
                getName(),
                getTimeGrain(),
                buildColumns(dictionaries.getDimensionDictionary()),
                getLogicalToPhysicalNames(),
                new PermissiveAvailability(DataSourceName.of(getName().asName()), metadataService), //todo correct?
                schemaName,
                timestampColumn
        );
    }
}
