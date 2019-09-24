// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.physicaltables.BaseCompositePhysicalTable;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.availability.PartitionAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter;
import com.yahoo.bard.webservice.table.resolver.DimensionIdFilter;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds the fields needed to define a partition physical table.
 */
public class DimensionListPartitionTableDefinition extends PhysicalTableDefinition {

    private final Map<TableName, Map<String, Set<String>>> tablePartDefinitions;

    /**
     * Constructor.
     *
     * @param name  Table name of the physical table
     * @param timeGrain  Zoned time grain of the table
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  Set of dimensions on the table as dimension configs
     * @param tablePartDefinitions  A map from table names to a map of dimension names to sets of values for those
     * dimensions.  The named table will match if for every dimension named at least one of the set of values is part
     * of the query.
     */
    public DimensionListPartitionTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<TableName, Map<String, Set<String>>> tablePartDefinitions
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs);
        this.tablePartDefinitions = tablePartDefinitions;
    }

    @Override
    public Set<TableName> getDependentTableNames() {
        return tablePartDefinitions.keySet();
    }

    @Override
    public ConfigPhysicalTable build(ResourceDictionaries dictionaries, DataSourceMetadataService metadataService) {
        Map<ConfigPhysicalTable, DataSourceFilter> availabilityFilters = tablePartDefinitions.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> dictionaries.getPhysicalDictionary().get(entry.getKey().asName()),
                        entry -> new DimensionIdFilter(toDimensionValuesMap(
                                entry.getValue(),
                                dictionaries.getDimensionDictionary()
                        ))
                ));

        return new BaseCompositePhysicalTable(
                getName(),
                getTimeGrain(),
                buildColumns(dictionaries.getDimensionDictionary()),
                availabilityFilters.keySet(),
                getLogicalToPhysicalNames(),
                PartitionAvailability.build(availabilityFilters)
        );
    }

    /**
     * Bind a map from String dimension names to dimension keys.
     *
     * @param dimensionNameMap  The configuration map from dimension names to sets of dimension key values.
     * @param dimensionDictionary  The dictionary of dimensions to use for binding.
     *
     * @return a map of dimensions to dimension key values.
     */
    private Map<Dimension, Set<String>> toDimensionValuesMap(
            Map<String, Set<String>> dimensionNameMap,
            DimensionDictionary dimensionDictionary
    ) {
        return dimensionNameMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> dimensionDictionary.findByApiName(entry.getKey()),
                        Map.Entry::getValue
                        )
                );
    }
}
