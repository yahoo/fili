// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.table.TableGroup;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A TableLoader from configuration.
 */
public class ConfiguredTableLoader extends BaseTableLoader {

    protected final ConfigurationDictionary<LogicalTableConfiguration> logicalTables;
    protected final ConfigurationDictionary<PhysicalTableConfiguration> physicalTables;
    protected final ConfigurationDictionary<DimensionConfig> dimensions;

    /**
     * Construct a table loader from configuration.
     *
     * @param logicalTables the logical tables
     * @param physicalTables the physical tables
     * @param dimensions the dimensions
     */
    public ConfiguredTableLoader(
            ConfigurationDictionary<LogicalTableConfiguration> logicalTables,
            ConfigurationDictionary<PhysicalTableConfiguration> physicalTables,
            ConfigurationDictionary<DimensionConfig> dimensions
    ) {
        this.logicalTables = logicalTables;
        this.physicalTables = physicalTables;
        this.dimensions = dimensions;
    }

    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {

        for (Map.Entry<String, LogicalTableConfiguration> entry : logicalTables.entrySet()) {
            LogicalTableConfiguration logicalTable = entry.getValue();
            String tableName = entry.getKey();

            // Compute the set of all physical fields on this logical table's physical tables
            Set<FieldName> physicalFields = logicalTable
                    .getPhysicalTables()
                    .stream()
                    .map(physicalTable -> physicalTables.get(physicalTable).getMetrics())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            // Build the actual physical tables for this logical table
            Set<PhysicalTableDefinition> physicalTableDefs = logicalTable
                    .getPhysicalTables()
                    .stream()
                    .map(physicalTable -> physicalTables.get(physicalTable).buildPhysicalTable(dimensions))
                    .collect(Collectors.toSet());

            // Get the smallest granularity for this table
            // TimeGrain smallestGrain = Collections.min(entry.getValue().timeGrains);

            // Fixme: We should use 'apiName' vs 'asName' as intended
            // FIXME: should be possible to override grains valid for?
            Set<ApiMetricName> apiMetricNames = new HashSet<>();
            for (String metric : logicalTable.getMetrics()) {
                apiMetricNames.add(new ApiMetricName() {
                    @Override
                    public boolean isValidFor(TimeGrain grain) {
                        return logicalTable.getTimeGrains()
                                .stream()
                                .anyMatch(grain::satisfiedBy);
                    }

                    @Override
                    public String getApiName() {
                        return metric;
                    }

                    @Override
                    public String asName() {
                        return metric;
                    }
                });
            }

            TableGroup tableGroup = buildTableGroup(
                    tableName,
                    apiMetricNames,
                    physicalFields,
                    physicalTableDefs,
                    dictionaries
            );

            loadLogicalTableWithGranularities(
                    entry.getKey(),
                    tableGroup,
                    new HashSet<>(entry.getValue().getTimeGrains()),
                    dictionaries
            );
        }
    }
}
