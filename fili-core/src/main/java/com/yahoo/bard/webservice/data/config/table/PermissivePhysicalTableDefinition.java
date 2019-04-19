// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.physicaltables.PermissivePhysicalTable;

import java.util.Map;
import java.util.Set;

/**
 * Holds the fields needed to define a Permissive Physical Table.
 */
public class PermissivePhysicalTableDefinition extends ConcretePhysicalTableDefinition {

    /**
     * Define a permissive physical table.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     */
    public PermissivePhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs);
    }

    /**
     * Define a permissive physical table with provided logical to physical column name mappings.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     * @param logicalToPhysicalNames  A map from logical column names to physical column names
     */
    public PermissivePhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<String, String> logicalToPhysicalNames

    ) {
        super(name, timeGrain, metricNames, dimensionConfigs, logicalToPhysicalNames);
    }


    @Override
    public ConfigPhysicalTable build(
            ResourceDictionaries dictionaries,
            DataSourceMetadataService metadataService
    ) {
        return new PermissivePhysicalTable(
                getName(),
                getTimeGrain(),
                buildColumns(dictionaries.getDimensionDictionary()),
                getLogicalToPhysicalNames(),
                metadataService
        );
    }
}
