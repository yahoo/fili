// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import com.google.common.collect.ImmutableSet;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An availability that provides column and table available interval services for concrete physical table.
 */
public class ConcreteAvailability implements Availability {

    private final TableName name;
    private final Set<Column> columns;
    private final DataSourceMetadataService metadataService;

    private final Set<String> columnNames;
    private final Set<TableName> dataSourceNames;

    /**
     * Constructor.
     *
     * @param tableName  The name of the table and data source associated with this Availability
     * @param columns  The columns associated with the table and availability
     * @param metadataService  A service containing the datasource segment data
     */
    public ConcreteAvailability(
            @NotNull TableName tableName,
            @NotNull Set<Column> columns,
            @NotNull DataSourceMetadataService metadataService
    ) {
        this.name = tableName;
        this.columns = ImmutableSet.copyOf(columns);
        this.metadataService = metadataService;

        this.columnNames = columns.stream().map(Column::getName).collect(Collectors.toSet());
        this.dataSourceNames = Collections.singleton(name);
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    @Override
    public Map<Column, List<Interval>> getAllAvailableIntervals() {

        Map<String, List<Interval>> allAvailableIntervals = getAvailableIntervalsByTable();
        return columns.stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                column -> new SimplifiedIntervalList(
                                        allAvailableIntervals.getOrDefault(column.getName(), Collections.emptyList())
                                )
                        )
                );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraint) {

        Set<String> requestColumns = constraint.getAllColumnNames().stream()
                .filter(columnNames::contains)
                .collect(Collectors.toSet());

        if (requestColumns.isEmpty()) {
            return new SimplifiedIntervalList();
        }

        Map<String, List<Interval>> allAvailableIntervals = getAvailableIntervalsByTable();

        // Need to ensure requestColumns is not empty in order to prevent returning null by reduce operation
        return new SimplifiedIntervalList(
                requestColumns.stream()
                        .map(columnName -> allAvailableIntervals.getOrDefault(columnName, Collections.emptyList()))
                        .map(intervals -> (Collection<Interval>) intervals)
                        .reduce(null, IntervalUtils::getOverlappingSubintervals)
        );
    }

    /**
     * Retrieves the most up to date column to available interval map from data source metadata service.
     *
     * @return map of column name to list of avialable intervals
     */
    protected Map<String, List<Interval>> getAvailableIntervalsByTable() {
        return metadataService.getAvailableIntervalsByTable(name);
    }

    protected Set<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public String toString() {
        return "concrete availability";
    }
}
