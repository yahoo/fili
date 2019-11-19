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
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.SqlPhysicalTable;
import com.yahoo.bard.webservice.table.availability.BaseMetadataAvailability;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link ConcretePhysicalTableDefinition} specific to SQL backed datasources.
 */
public class ConcreteSqlPhysicalTableDefinition extends ConcretePhysicalTableDefinition {
    private final String schemaName;
    private final String timestampColumn;
    private final String catalog;

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
        this.catalog = null;
    }

    /**
     * Define a sql backed physical table using a zoned time grain. Requires the schema and timestamp column to be
     * specified.
     *
     * @param schemaName  The name of sql schema this table is on.
     * @param timestampColumn  The name of the timestamp column to be used for the database.
     * @param catalog The name of the database
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     */
    public ConcreteSqlPhysicalTableDefinition(
            String schemaName,
            String timestampColumn,
            String catalog,
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs);
        this.schemaName = schemaName;
        this.timestampColumn = timestampColumn;
        this.catalog = catalog;
    }

    /**
     * Define a physical table with provided logical to physical column name mappings. Requires the schema and timestamp
     * column to be specified.
     *
     * @param schemaName  The name of sql schema this table is on.
     * @param timestampColumn  The name of the timestamp column to be used for the database.
     * @param catalog The name of the database
     * @param name  The table name
     * @param timeGrain  The zoned time grain
     * @param metricNames  The Set of metric names on the table
     * @param dimensionConfigs  The dimension configurations
     * @param logicalToPhysicalNames  A map from logical column names to physical column names
     */
    public ConcreteSqlPhysicalTableDefinition(
            String schemaName,
            String timestampColumn,
            String catalog,
            TableName name,
            ZonedTimeGrain timeGrain,
            Set<FieldName> metricNames,
            Set<? extends DimensionConfig> dimensionConfigs,
            Map<String, String> logicalToPhysicalNames
    ) {
        super(name, timeGrain, metricNames, dimensionConfigs, logicalToPhysicalNames);
        this.schemaName = schemaName;
        this.timestampColumn = timestampColumn;
        this.catalog = catalog;
    }

    @Override
    public ConfigPhysicalTable build(ResourceDictionaries dictionaries, DataSourceMetadataService metadataService) {
        return new SqlPhysicalTable(
                getName(),
                getTimeGrain(),
                buildColumns(dictionaries.getDimensionDictionary()),
                getLogicalToPhysicalNames(),
                new EternalAvailability(DataSourceName.of(getName().asName()), metadataService),
                schemaName,
                timestampColumn,
                catalog
        );
    }

    /**
     * Provides availability over {@link #ETERNITY}.
     */
    private static class EternalAvailability extends BaseMetadataAvailability {
        private static final long MAX_INSTANT = Long.MAX_VALUE / 2;
        private static final long MIN_INSTANT = Long.MIN_VALUE / 2;
        private static final SimplifiedIntervalList ETERNITY = new SimplifiedIntervalList(
                Collections.singletonList(new Interval(MIN_INSTANT, MAX_INSTANT))
        );
        private static final Map<String, SimplifiedIntervalList> ALL_COLUMNS_ETERNAL_AVAILABILITY =
                new HashMap<String, SimplifiedIntervalList>() {
                    @Override
                    public SimplifiedIntervalList get(Object key) {
                        return ETERNITY;
                    }
                };

        /**
         * Constructor an availability which is valid over {@link #ETERNITY}.
         *
         * @param dataSourceName  The name of the data source associated with this Availability
         * @param metadataService  A service containing the datasource segment data
         */
        EternalAvailability(DataSourceName dataSourceName, DataSourceMetadataService metadataService) {
            super(dataSourceName, metadataService);
        }

        @Override
        public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
            return ETERNITY;
        }

        @Override
        public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
            return ALL_COLUMNS_ETERNAL_AVAILABILITY;
        }
    }

    /**
     * Gets the sql schema name this table belongs to.
     *
     * @return the schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Gets the catalog. Catalog is the database name.
     *
     * @return the catalog name.
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Gets the name of the timestamp column backing this table.
     *
     * @return the name of the timestamp column.
     */
    public String getTimestampColumn() {
        return timestampColumn;
    }
}
