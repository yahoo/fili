// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.fili.webservice.data.config.table;

import com.yahoo.fili.webservice.data.config.ResourceDictionaries;
import com.yahoo.fili.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.fili.webservice.data.config.names.FieldName;
import com.yahoo.fili.webservice.data.config.names.TableName;
import com.yahoo.fili.webservice.data.time.ZonedTimeGrain;
import com.yahoo.fili.webservice.metadata.DataSourceMetadataService;
import com.yahoo.fili.webservice.table.ConfigPhysicalTable;
import com.yahoo.fili.webservice.table.MetricUnionCompositeTable;
import com.yahoo.fili.webservice.table.PhysicalTableDictionary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds the fields needed to define a Metric Union Composite Table.
 */
public class MetricUnionCompositeTableDefinition extends PhysicalTableDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(MetricUnionCompositeTableDefinition.class);

    private final Set<TableName> dependentTableNames;

    /**
     * Define a physical table using a zoned time grain.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dependentTableNames  The set of dependent table names on the table
     * @param dimensionConfigs  The dimension configurations
     */
    public MetricUnionCompositeTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<TableName> dependentTableNames,
            Set<? extends DimensionConfig> dimensionConfigs
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs);
        this.dependentTableNames = dependentTableNames;
    }

    /**
     * Define a physical table with provided logical to physical column name mappings.
     *
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dependentTableNames  The set of dependent table names on the table
     * @param dimensionConfigs  The dimension configurations
     * @param logicalToPhysicalNames  A map from logical column names to physical column names
     */
    public MetricUnionCompositeTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<TableName> dependentTableNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<String, String> logicalToPhysicalNames

    ) {
        super(name, timeGrain, metricNames, dimensionConfigs, logicalToPhysicalNames);
        this.dependentTableNames = dependentTableNames;
    }

    @Override
    public Set<TableName> getDependentTableNames() {
        return dependentTableNames;
    }

    @Override
    public ConfigPhysicalTable build(ResourceDictionaries dictionaries, DataSourceMetadataService metadataService) {
        return new MetricUnionCompositeTable(
                getName(),
                getTimeGrain(),
                buildColumns(dictionaries.getDimensionDictionary()),
                getPhysicalTables(dictionaries),
                getLogicalToPhysicalNames()
        );
    }

    /**
     * Returns set of PhysicalTables from ResourceDictionaries.
     *
     * @param resourceDictionaries  The ResourceDictionaries from which the tables are to be retrieved
     *
     * @return set of PhysicalTables from the ResourceDictionaries
     */
    private Set<ConfigPhysicalTable> getPhysicalTables(ResourceDictionaries resourceDictionaries) {
        PhysicalTableDictionary physicalTableDictionary = resourceDictionaries.getPhysicalDictionary();
        return dependentTableNames.stream()
                .map(TableName::asName)
                .peek(name -> {
                    if (physicalTableDictionary.get(name) == null) {
                        LOG.error(
                                "{} is needed to build {}, but it's not found in ResourceDictionaries in" +
                                        "MetricUnionCompositeTableDefinition",
                                name,
                                getName().asName()
                        );
                    }
                })
                .map(physicalTableDictionary::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
}
