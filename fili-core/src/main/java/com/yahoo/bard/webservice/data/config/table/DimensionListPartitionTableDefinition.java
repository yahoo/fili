// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.table;

import static com.yahoo.bard.webservice.util.DateTimeUtils.EARLIEST_DATETIME;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionDictionary;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.PartitionCompositeTable;
import com.yahoo.bard.webservice.table.availability.Availability;
import com.yahoo.bard.webservice.table.resolver.DataSourceFilter;
import com.yahoo.bard.webservice.table.resolver.DimensionIdFilter;

import org.apache.commons.collections4.map.DefaultedMap;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds the fields needed to define a partition physical table.
 * <p>
 * {@link DimensionListPartitionTableDefinition} supports physical-table-Specific time limit in
 * {@link PartitionCompositeTable}. This feature is explained below:
 * <p>
 * If a Druid instance has 2 datasources DS1 and DS2 that have the following availabilities for some metric M, and they
 * are both behind a single composite table
 * <pre>
 * +-----+-----------------------+-----------------------+----------+
 * |     | 2017-01-01/2017-12-31 | 2018-01-01/2018-12-31 |  FUTURE  |
 * +-----+-----------------------+-----------------------+----------+
 * | DS1 |        HAS DATA       |        HAS DATA       | HAS DATA |
 * +-----+-----------------------+-----------------------+----------+
 * | DS2 |        NO DATA        |        HAS DATA       | HAS DATA |
 * +-----+-----------------------+-----------------------+----------+
 * </pre>
 * Imagine DS2 is a new datasource added to Druid in 2018-01-01. Then DS2 is defined as starting on interval
 * 2018/FUTURE.
 * <p>
 * If we query M on a groupBy dimension D in year 2017, data from DS1 will still be marked as available, even though
 * this is not the case for DS2.
 * <p>
 * A summary of the behavior is the following:
 * <ul>
 *     <li>If DS1 is included for filter set X and DS2 is also included for filter set X, and</li>
 *     <li>DS2 is defined as starting on interval 2018/FUTURE</li>
 * </ul>
 * then the availability is
 * <ul>
 *     <li>Requested Interval is "THE PAST/2018" {@literal =>} Intersect: all tables in range (DS1)</li>
 *     <li>Requested Interval is "2018/THE FUTURE" {@literal =>} Intersect: all tables in range (DS1, DS2)</li>
 * </ul>
 * <p>
 * {@link DimensionListPartitionTableDefinition} defines a mark "T" for each participating table in
 * {@link PartitionCompositeTable}. The "mark" indicates <b>a starting instance of time, T, after which data can
 * possibly be available.</b>
 * <p>
 * With the "mark", the decision on "missing intervals" is the following:
 * <ul>
 *     <li>
 *         If there is no data in this interval AND this interval is <b>before</b> T {@code =>} NOT a missing interval;
 *         do not include it in the availability intersection operation
 *     </li>
 *     <li>
 *         If there is no data in this interval AND this interval is <b>after</b> T {@code =>} this IS a missing
 *         interval and will be part of intersection operations
 *     </li>
 * </ul>
 * <p>
 * <b>The value of T of each partition will be configured and loaded on start. This value can be provided via
 * {@link #DimensionListPartitionTableDefinition(TableName, ZonedTimeGrain, Set, Set, Map, Map)}.</b> If you don't need
 * this feature, call construct via
 * {@link #DimensionListPartitionTableDefinition(TableName, ZonedTimeGrain, Set, Set, Map)} and the "marks" of all
 * tables will be 1970-01-01.
 */
public class DimensionListPartitionTableDefinition extends PhysicalTableDefinition {

    private final Map<TableName, Map<String, Set<String>>> tablePartDefinitions;
    private final Map<TableName, DateTime> tableStartDate;

    /**
     * Constructor.
     * <p>
     * This constructor takes an parameter that maps a table to a starting instance of time after which data can
     * possibly be available. <b>If a mapping for a table is not provided, the instance will default to
     * {@link com.yahoo.bard.webservice.util.DateTimeUtils#EARLIEST_DATETIME}</b>
     *
     * @param name  Table name of the physical table
     * @param timeGrain  Zoned time grain of the table
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  Set of dimensions on the table as dimension configs
     * @param tablePartDefinitions  A map from table names to a map of dimension names to sets of values for those
     * dimensions. The named table will match if for every dimension named at least one of the set of values is part
     * of the query.
     * @param tableStartDate  The map that maps a table to a starting instance of time after which data can possibly be
     * available.
     */
    public DimensionListPartitionTableDefinition(
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<TableName, Map<String, Set<String>>> tablePartDefinitions,
            Map<TableName, DateTime> tableStartDate
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs);
        this.tablePartDefinitions = tablePartDefinitions;
        this.tableStartDate = tableStartDate;
    }

    /**
     * Constructor.
     * <p>
     * This constructor assumes that unavailable data is the same thing as missing data. See
     * {@link #DimensionListPartitionTableDefinition(TableName, ZonedTimeGrain, Set, Set, Map, Map)}.
     *
     * @param name  Table name of the physical table
     * @param timeGrain  Zoned time grain of the table
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  Set of dimensions on the table as dimension configs
     * @param tablePartDefinitions  A map from table names to a map of dimension names to sets of values for those
     * dimensions. The named table will match if for every dimension named at least one of the set of values is part
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
        this.tableStartDate = new DefaultedMap<>(EARLIEST_DATETIME);
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
        Map<Availability, DateTime> availabilityStartDate = tablePartDefinitions.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                entry -> dictionaries
                                        .getPhysicalDictionary()
                                        .get(entry.getKey().asName())
                                        .getAvailability(),
                                entry -> tableStartDate.getOrDefault(entry.getKey(), EARLIEST_DATETIME)
                        )
                );

        return new PartitionCompositeTable(
                getName(),
                getTimeGrain(),
                buildColumns(dictionaries.getDimensionDictionary()),
                getLogicalToPhysicalNames(),
                availabilityFilters,
                availabilityStartDate
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
