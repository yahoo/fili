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
import com.yahoo.bard.webservice.table.physicaltables.StrictPhysicalTable;

import org.joda.time.DateTime;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Holds the fields needed to define a Concrete Physical Table.
 */
public class ConcretePhysicalTableDefinition extends PhysicalTableDefinition {

    /**
     * Define a physical table using a zoned time grain. Defaults to table with no expected start nor end dates.
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
        super(name, timeGrain, metricNames, dimensionConfigs, null, null);
    }

    /**
     * Define a physical table using a zoned time grain.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     * @param expectedStartDate  The expected start date of the datasource the constructed table will represent. Null
     * indicates there is NO expected start date
     * @param expectedEndDate  The expected end date of the datasource the constructed table will represent. Null
     * indicates there is NO expected end date
     */
    public ConcretePhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            DateTime expectedStartDate,
            DateTime expectedEndDate
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs, expectedStartDate, expectedEndDate);
    }

    /**
     * Define a physical table with provided logical to physical column name mappings. Defaults to config with no
     * expected start nor end dates.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     * @param logicalToPhysicalNames  A map from logical column names to physical column names
     */
    public ConcretePhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<String, String> logicalToPhysicalNames
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs, logicalToPhysicalNames, null, null);
    }

    /**
     * Define a physical table with provided logical to physical column name mappings.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     * @param logicalToPhysicalNames  A map from logical column names to physical column names
     * @param expectedStartDate  The expected start date of the datasource the constructed table will represent. Null
     * indicates there is NO expected start date
     * @param expectedEndDate  The expected end date of the datasource the constructed table will represent. Null
     * indicates there is NO expected end date
     */
    public ConcretePhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<String, String> logicalToPhysicalNames,
            DateTime expectedStartDate,
            DateTime expectedEndDate
    ) {
        super(
                name,
                timeGrain,
                metricNames,
                dimensionConfigs,
                logicalToPhysicalNames,
                expectedStartDate,
                expectedEndDate
        );
    }

    @Override
    public Set<TableName> getDependentTableNames() {
        return Collections.emptySet();
    }

    @Override
    public ConfigPhysicalTable build(ResourceDictionaries dictionaries, DataSourceMetadataService metadataService) {
        return new StrictPhysicalTable(
                getName(),
                getTimeGrain(),
                buildColumns(dictionaries.getDimensionDictionary()),
                getLogicalToPhysicalNames(),
                metadataService,
                getExpectedStartDate(),
                getExpectedEndDate()
        );
    }
}
