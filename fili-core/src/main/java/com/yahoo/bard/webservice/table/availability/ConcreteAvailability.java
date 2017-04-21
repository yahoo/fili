// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An availability that provides column and table available interval services for concrete physical table.
 */
public class ConcreteAvailability implements Availability {

    private final TableName name;
    private final DataSourceMetadataService metadataService;

    private final Set<TableName> dataSourceNames;

    /**
     * Constructor.
     *
     * @param tableName  The name of the table and data source associated with this Availability
     * @param metadataService  A service containing the datasource segment data
     */
    public ConcreteAvailability(
            @NotNull TableName tableName,
            @NotNull DataSourceMetadataService metadataService
    ) {
        this.name = tableName;
        this.metadataService = metadataService;

        this.dataSourceNames = Collections.singleton(name);
    }

    @Override
    public Set<TableName> getDataSourceNames() {
        return dataSourceNames;
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return metadataService.getAvailableIntervalsByTable(name);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {

        Set<String> requestColumns = constraint.getAllColumnPhysicalNames();

        if (requestColumns.isEmpty()) {
            return new SimplifiedIntervalList();
        }

        // Need to ensure requestColumns is not empty in order to prevent returning null by reduce operation
        return requestColumns.stream()
                .map(physicalName -> getAllAvailableIntervals().getOrDefault(
                        physicalName,
                        new SimplifiedIntervalList()
                )).reduce(SimplifiedIntervalList::intersect).orElse(new SimplifiedIntervalList());
    }


    /**
     * Returns the name of the table and data source associated with this Availability.
     *
     * @return the name of the table and data source associated with this Availability
     */
    protected TableName getName() {
        return name;
    }

    @Override
    public String toString() {
        return String.format("ConcreteAvailability for table: %s", name.asName()
        );
    }
}
