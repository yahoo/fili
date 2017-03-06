// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.Column;
import com.yahoo.bard.webservice.table.resolver.DataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An availability which allows missing intervals, i.e. returns union of available intervals, on its contents.
 */
public class PermissiveAvailability extends ConcreteAvailability {

    /**
     * Constructor.
     *
     * @param tableName The name of the data source
     * @param columns A set of columns associated with the data source
     * @param metadataService A service containing the data source segment data
     */
    public PermissiveAvailability(
            @NotNull TableName tableName,
            @NotNull Set<Column> columns,
            @NotNull DataSourceMetadataService metadataService
    ) {
        super(tableName, columns, metadataService);
    }

    /**
     * Returns union of all available intervals.
     * <p>
     * This is different from its parent's
     * {@link
     * com.yahoo.bard.webservice.table.availability.ConcreteAvailability#getAvailableIntervals(DataSourceConstraint)};
     * Instead of returning the intersection of all available intervals, this method returns the union of them.
     *
     * @param constraints  Data constraint containing columns and api filters
     *
     * @return the union of all available intervals
     */
    @Override
    public SimplifiedIntervalList getAvailableIntervals(DataSourceConstraint constraints) {
        Map<String, List<Interval>> allAvailableIntervals = getMetadataService()
                .getAvailableIntervalsByTable(getName());
        return new SimplifiedIntervalList(
                getCachedColumnNames().stream()
                        .map(columnName -> allAvailableIntervals.getOrDefault(columnName, Collections.emptyList()))
                        .flatMap(List::stream)
                        .collect(SimplifiedIntervalList.getCollector())
        );
    }
}
