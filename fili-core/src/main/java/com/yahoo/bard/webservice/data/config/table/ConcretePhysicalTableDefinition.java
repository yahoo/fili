// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.ConcretePhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTable;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Holds the fields needed to define a Concrete Physical Table.
 */
public class ConcretePhysicalTableDefinition extends PhysicalTableDefinition {

    /**
     * Define a physical table using a zoned time grain.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     */
    public ConcretePhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs);
    }

    @Override
    public Set<TableName> getDependentTableNames() {
        return Collections.emptySet();
    }

    @Override
    public Optional<PhysicalTable> build(
            ResourceDictionaries dictionaries,
            DataSourceMetadataService metadataService
    ) {
        return Optional.of(
                new ConcretePhysicalTable(
                        getName(),
                        getTimeGrain(),
                        buildColumns(dictionaries.getDimensionDictionary()),
                        getLogicalToPhysicalNames(),
                        metadataService
                )
        );
    }
}
