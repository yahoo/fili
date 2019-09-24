// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.metric.MetricColumn;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.physicaltables.BaseCompositePhysicalTable;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.PhysicalTableDictionary;
import com.yahoo.bard.webservice.table.Schema;
import com.yahoo.bard.webservice.table.Table;
import com.yahoo.bard.webservice.table.availability.MetricUnionAvailability;
import com.yahoo.bard.webservice.util.Utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Holds the fields needed to define a Metric Union Composite Table.
 */
public class MetricUnionCompositeTableDefinition extends PhysicalTableDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(MetricUnionCompositeTableDefinition.class);

    private final Set<TableName> dependentTableNames;
    private final Set<String> dependentTableNameString;

    public static final String MISSING_DEPENDANT_TABLE_FORMAT = "Dependent able %s does not exist.";
    public static final String MISSING_METRIC_FORMAT = "Required metric %s doesn't appear on any dependent table";
    public static final String DUPLICATE_METRIC_FORMAT = "Required metric(s) %s appears on more " +
            "than one dependent table";

    public static final String VALIDATION_ERROR_FORMAT = "Error building table %s: %s";

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
        this.dependentTableNameString = dependentTableNames.stream().map(TableName::asName).collect(Collectors.toSet());
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
        this.dependentTableNameString = dependentTableNames.stream().map(TableName::asName).collect(Collectors.toSet());
    }

    @Override
    public Set<TableName> getDependentTableNames() {
        return dependentTableNames;
    }

    @Override
    public ConfigPhysicalTable build(ResourceDictionaries dictionaries, DataSourceMetadataService metadataService) {
        try {
            Map<ConfigPhysicalTable, Set<String>> tableMetricNamesMap = getTableToMetricsMap(dictionaries);
            validateDependentMetrics(tableMetricNamesMap);
            return new BaseCompositePhysicalTable(
                    getName(),
                    getTimeGrain(),
                    buildColumns(dictionaries.getDimensionDictionary()),
                    tableMetricNamesMap.keySet(),
                    getLogicalToPhysicalNames(),
                    MetricUnionAvailability.build(
                            tableMetricNamesMap.keySet(), tableMetricNamesMap.entrySet().stream()
                                    .collect(Collectors.toMap(
                                            entry -> entry.getKey().getAvailability(),
                                            Map.Entry::getValue
                                    )))
            );
        } catch (IllegalArgumentException e) {
            String message = String.format(VALIDATION_ERROR_FORMAT, getName(), e.getMessage());
            LOG.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * Transforms dependent table names to metrics.
     *
     * @param tableNames  The names of the tables to be mapped
     * @param physicalTableDictionary  The physical table dictionary to resolve the names
     *
     * @return set of PhysicalTables from the ResourceDictionaries
     */
    private static Set<ConfigPhysicalTable> mapNamestoTables(
            Collection<String> tableNames,
            PhysicalTableDictionary physicalTableDictionary
    ) {
        Set<String> missingTableNames = tableNames.stream()
                .filter(it -> ! physicalTableDictionary.containsKey(it))
                .collect(Collectors.toSet());

        if (!missingTableNames.isEmpty()) {
            String message = String.format(MISSING_DEPENDANT_TABLE_FORMAT, missingTableNames);
            throw new IllegalArgumentException(message);
        }
        return tableNames.stream()
                .map(physicalTableDictionary::get)
                .collect(Collectors.toSet());
    }

    /**
     * Transforms dependent table names to metrics.
     *
     * @param tableMetricMap  The table to metric names map for the union
     *
     * @throws IllegalArgumentException If the availabilityMap is not supported for these tables
     */
    private static void validateDependentMetrics (Map<ConfigPhysicalTable, Set<String>> tableMetricMap) {
        Map<String, Long> frequency = tableMetricMap.entrySet().stream()
                .map(Map.Entry::getValue)
                .flatMap(Set::stream)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        Set<String> duplicateMetrics = frequency.entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (!duplicateMetrics.isEmpty()) {
            String message = String.format(DUPLICATE_METRIC_FORMAT, duplicateMetrics);
            throw new IllegalArgumentException(message);
        }

        Set<String> tableColumnNames = tableMetricMap.keySet().stream()
                .map(Table::getSchema)
                .map(Schema::getColumns)
                .flatMap(Set::stream)
                .map(Column::getName)
                .collect(Collectors.toSet());

        Set<String> expectedColumns = frequency.keySet();
        expectedColumns.removeAll(tableColumnNames);

        if (!expectedColumns.isEmpty()) {
            String message = String.format(MISSING_METRIC_FORMAT, duplicateMetrics);
            throw new IllegalArgumentException(message);
        }
    }


    /**
     * Returns a map from availability to set of metrics.
     *
     * @param dictionaries  The ResourceDictionaries from which the tables are to be retrieved
     *
     * @return A map from <tt>Availability</tt> to set of <tt>MetricColumn</tt>
     */
    public Map<ConfigPhysicalTable, Set<String>> getTableToMetricsMap(ResourceDictionaries dictionaries) {
        Set<Column> columns = buildColumns(dictionaries.getDimensionDictionary());
        Set<String> metricNames = Utils.getSubsetByType(columns, MetricColumn.class).stream()
                .map(MetricColumn::getName)
                .collect(Collectors.collectingAndThen(Collectors.toSet(), ImmutableSet::copyOf));

        Set<ConfigPhysicalTable> tables = mapNamestoTables(
                dependentTableNameString,
                dictionaries.getPhysicalDictionary()
        );
        // Construct a map of tables to the metrics for that table
        return tables.stream()
                .collect(
                        Collectors.collectingAndThen(Collectors.toMap(
                                Function.identity(),
                                (ConfigPhysicalTable physicalTable) ->
                                        Sets.intersection(
                                                physicalTable.getSchema().getMetricColumnNames(),
                                                metricNames
                                        )
                        ), ImmutableMap::copyOf)
                );
    }
}
