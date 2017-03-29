// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.DimensionColumn;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.PhysicalTable;

import avro.shaded.com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Common configurations necessary to build a physical table.
 */
public abstract class PhysicalTableDefinition {

    private final TableName name;
    private final ZonedTimeGrain timeGrain;
    private final Set<FieldName> metricNames;
    private final Set<? extends DimensionConfig> dimensionConfigs;
    private final Map<String, String> logicalToPhysicalNames;

    /**
     * Constructor for sub-class to call.
     *
     * @param name  Table name of the physical table
     * @param timeGrain  Zoned time grain of the table
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  Set of dimensions on the table as dimension configs
     */
    protected PhysicalTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs
    ) {
        this.name = name;
        this.timeGrain = timeGrain;
        this.metricNames = ImmutableSet.copyOf(metricNames);
        this.dimensionConfigs = ImmutableSet.copyOf(dimensionConfigs);
        this.logicalToPhysicalNames = Collections.unmodifiableMap(buildLogicalToPhysicalNames(dimensionConfigs));
    }

    /**
     * Get the set of physical tables required to build the current physical table.
     *
     * @return a set of physical table names
     */
    public abstract Set<TableName> getDependentTableNames();

    /**
     * Given a set of metric names and a data source metadata service, build the corresponding physical table.
     *
     * @param dictionaries  Dictionary containing dimension dictionary and physical table dictionary
     * @param metadataService  Service containing column available interval information
     *
     * @return optional of the corresponding type of physical table, empty optional if could not be build.
     */
    public abstract Optional<PhysicalTable> build(
            ResourceDictionaries dictionaries,
            DataSourceMetadataService metadataService
    );

    public TableName getName() {
        return name;
    }

    public ZonedTimeGrain getTimeGrain() {
        return timeGrain;
    }

    public Set<FieldName> getMetricNames() {
        return metricNames;
    }

    public Set<? extends DimensionConfig> getDimensionConfigs() {
        return dimensionConfigs;
    }

    public Map<String, String> getLogicalToPhysicalNames() {
        return logicalToPhysicalNames;
    }

    /**
     * Builds the dimension logical name to physical name mapping from dimension configs.
     *
     * @param dimensionConfigs  Dimension config containing both logical and physical names
     *
     * @return the dimension logical name to physical name mapping
     */
    protected Map<String, String> buildLogicalToPhysicalNames(Set<? extends DimensionConfig> dimensionConfigs) {
        return dimensionConfigs.stream()
                .collect(
                        Collectors.toMap(
                                DimensionConfig::getApiName,
                                config -> {
                                    String physicalName = config.getPhysicalName();
                                    if (physicalName == null) {
                                        throw new RuntimeException("No physical name found for dimension: "
                                                + config.getApiName());
                                    }
                                    return physicalName;
                                }
                        )

                );
    }

    /**
     * Helper method for sub-classes to convert dimension configs into dimension columns and create metric columns.
     *
     * @param dimensionDictionary  Dictionary for dimension name to dimension columns
     *
     * @return all columns including dimension columns and metric columns
     */
    protected Set<Column> buildColumns(DimensionDictionary dimensionDictionary) {
        return Stream.concat(
                // Load the dimension columns
                getDimensionConfigs().stream()
                        .map(DimensionConfig::getApiName)
                        .map(dimensionDictionary::findByApiName)
                        .map(DimensionColumn::new),
                // And the metric columns
                getMetricNames().stream()
                        .map(FieldName::asName)
                        .map(MetricColumn::new)
        ).collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
