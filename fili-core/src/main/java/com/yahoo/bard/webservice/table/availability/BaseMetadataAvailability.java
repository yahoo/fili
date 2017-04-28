// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An availability based on a DataSourceMetadataService backed by a single data source.
 */
public abstract class BaseMetadataAvailability implements Availability {

    private final DataSourceName dataSourceName;
    private final Set<DataSourceName> dataSourceNames;
    private final DataSourceMetadataService metadataService;

    /**
     * Constructor.
     *
     * @param dataSourceName  The name of the data source associated with this Availability
     * @param metadataService  A service containing the datasource segment data
     */
    public BaseMetadataAvailability(
            @NotNull DataSourceName dataSourceName,
            @NotNull DataSourceMetadataService metadataService
    ) {
        this.metadataService = metadataService;
        this.dataSourceName = dataSourceName;
        this.dataSourceNames = Collections.singleton(dataSourceName);
    }

    public DataSourceName getDataSourceName() {
        return dataSourceName;
    }

    @Override
    public Set<DataSourceName> getDataSourceNames() {
        return dataSourceNames;
    }

    public DataSourceMetadataService getDataSourceMetadataService() {
        return metadataService;
    }

    @Override
    public Map<String, SimplifiedIntervalList> getAllAvailableIntervals() {
        return getDataSourceMetadataService().getAvailableIntervalsByDataSource(getDataSourceName());
    }

    @Override
    public abstract SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint);

    @Override
    public String toString() {
        return String.format("BaseMetadataAvailability for data source = %s", getDataSourceName().asName());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BaseMetadataAvailability && this.getClass().equals(obj.getClass())) {
            BaseMetadataAvailability that = (BaseMetadataAvailability) obj;
            return Objects.equals(getDataSourceName().asName(), that.getDataSourceName().asName())
                    // Since metadata service is mutable, use instance equality to ensure table equality is stable
                    && getDataSourceMetadataService() == that.getDataSourceMetadataService();
        }
        return false;
    }

    @Override
    public int hashCode() {
        // Leave metadataService out of hash because it is mutable
        return Objects.hash(getDataSourceName());
    }
}
