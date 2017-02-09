// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.IntervalUtils;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * An availability which guarantees immutability on its contents.
 */
public class ConcreteAvailability implements Availability {

    private final TableName name;
    private final Set<Column> columns;
    private final DataSourceMetadataService metadataService;

    /**
     * Constructor.
     *
     * @param tableName The name of the data source associated with this ImmutableAvailability
     * @param columns The columns associated with the table and availability
     * @param metadataService A service containing the datasource segment data
     */
    public ConcreteAvailability(
            TableName tableName,
            @NotNull Set<Column> columns,
            @NotNull DataSourceMetadataService metadataService
    ) {
        this.name = tableName;
        this.columns = columns;
        this.metadataService = metadataService;
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return Collections.singleton(name);
    }

    @Override
    public Map<Column, Set<Interval>> getAllAvailableIntervals() {

        Map<String, Set<Interval>> allAvailableIntervals = metadataService.getAvailableIntervalsByTable(name);
        return columns.stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                column -> allAvailableIntervals.getOrDefault(column.getName(), Collections.emptySet())
                        )
                );
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraints) {

        Set<String> configuredColumns = columns.stream()
                .map(Column::getName).collect(Collectors.toSet());

        Set<String> requestColumns = constraints.getAllColumnNames().stream()
                .filter(requestColumn -> configuredColumns.contains(requestColumn)).collect(Collectors.toSet());

        if (requestColumns.isEmpty()) {
            return new SimplifiedIntervalList();
        }

        Map<String, Set<Interval>> allAvailableIntervals = metadataService.getAvailableIntervalsByTable(name);
        return new SimplifiedIntervalList(
                requestColumns.stream()
                        .map(columnName -> allAvailableIntervals.getOrDefault(columnName, Collections.emptySet()))
                        .reduce(null, IntervalUtils::getOverlappingSubintervals)
        );
    }
}
