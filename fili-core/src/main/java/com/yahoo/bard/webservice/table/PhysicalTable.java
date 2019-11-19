// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.DateTime;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An interface describing a fact level physical table. It may be backed by a single fact table or multiple.
 */
public interface PhysicalTable extends Table {
    /**
     * Getter for all the available intervals for the corresponding columns configured on the table.
     *
     * @return map of column to set of available intervals
     */
    Map<Column, SimplifiedIntervalList> getAllAvailableIntervals();

    /**
     * Return a view of the available intervals.
     *
     * @return The widest set of intervals that the table can report on
     */
    default SimplifiedIntervalList getAvailableIntervals() {
        // By default union all available columns
        return getAllAvailableIntervals().values().stream()
                .reduce(SimplifiedIntervalList::union)
                .orElse(new SimplifiedIntervalList());
    }

    /**
     * Get the name of the table.
     *
     * @return name of the table as TableName
     *
     * @deprecated  Use Table::getName instead
     */
    @Deprecated
    TableName getTableName();

    /**
     * Return a view of the available intervals for this table given a constraint.
     *
     * @param constraint  The constraint which limits available intervals
     *
     * @return The widest set of intervals that the table can report on, given the constraints
     */
    default SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {
        // Default to unconstrained
        return getAvailableIntervals();
    }

    /**
     * Get the columns from the schema for this physical table.
     *
     * @return The columns of this physical table
     *
     * @deprecated In favor of getting the columns directly from the schema
     */
    @Deprecated
    default Set<Column> getColumns() {
        return getSchema().getColumns();
    }

    /**
     * Get the names of the data sources that back this physical table.
     *
     * @return the names of all data sources that back this physical table.
     */
    Set<DataSourceName> getDataSourceNames();

    /**
     * Return the {@link DataSourceName} of the dataSources which back this table given a constraint.
     *
     * @param constraint  A constraint which may narrow the data sources participating.
     *
     * @return A set of names for backing dataSources, given the constraints
     */
    default Set<DataSourceName> getDataSourceNames(DataSourceConstraint constraint) {
        return getDataSourceNames();
    }

    /**
     * Translate a logical name into a physical column name. If no translation exists (i.e. they are the same),
     * then the logical name is returned.
     * <p>
     * NOTE: This defaulting behavior <em>WILL BE REMOVED</em> in future releases.
     * <p>
     * The defaulting behavior shouldn't be hit for Dimensions that are serialized via the default serializer and are
     * not properly configured with a logical-to-physical name mapping. Dimensions that are not "normal" dimensions,
     * such as dimensions used for DimensionSpecs in queries to do mapping from fact-level dimensions to something else,
     * should likely use their own serialization strategy so as to not hit this defaulting behavior.
     *
     * @param logicalName  Logical name to lookup in physical table
     *
     * @return Translated logicalName if applicable
     */
    String getPhysicalColumnName(String logicalName);

    @Override
    PhysicalTableSchema getSchema();

    /**
     * Get a date time that the table will align to based on grain and available intervals.
     *
     * @return The time of either the first available interval of any columns in this table or now, floored to the
     * table's time grain.
     */
    DateTime getTableAlignment();

    /**
     * Create a constrained copy of this table.
     *
     * @param constraint  The dataSourceConstraint which narrows the view of the underlying availability
     *
     * @return a constrained table whose availability and serialization are narrowed by this constraint
     */
    ConstrainedTable withConstraint(DataSourceConstraint constraint);

    /**
     * Map availabilities in schema-less columns to a {@link Column} keyed availability map for a given table.
     *
     * @param rawIntervals  The map of physical name to {@link SimplifiedIntervalList}s as the source availability
     * @param schema  The schema describing the columns of this table, which includes the logical -&gt; physical
     * mappings
     *
     * @return map of column to set of available intervals
     */
    default Map<Column, SimplifiedIntervalList> mapToSchemaAvailability(
            Map<String, SimplifiedIntervalList> rawIntervals,
            PhysicalTableSchema schema
    ) {
        return schema.getColumns().stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                column -> rawIntervals.getOrDefault(
                                        schema.getPhysicalColumnName(column.getName()),
                                        new SimplifiedIntervalList()
                                )
                        )
                );
    }
}
