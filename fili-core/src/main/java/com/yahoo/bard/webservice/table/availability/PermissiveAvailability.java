// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import javax.validation.constraints.NotNull;

/**
 * An availability which allows missing intervals, i.e. returns union of available intervals, on its contents.
 * This availability returns available intervals without restrictions from <tt>DataSourceConstraint</tt>, because the
 * nature of this availability is to returns as many available intervals as possible.
 */
public class PermissiveAvailability extends ConcreteAvailability {
    /**
     * Constructor.
     *
     * @param tableName  The name of the data source
     * @param metadataService  A service containing the data source segment data
     */
    public PermissiveAvailability(
            @NotNull TableName tableName,
            @NotNull DataSourceMetadataService metadataService
    ) {
        super(tableName, metadataService);
    }

    /**
     * Returns union of all available intervals.
     * <p>
     * This is different from its parent's
     * {@link
     * com.yahoo.bard.webservice.table.availability.ConcreteAvailability#getAvailableIntervals(
     * PhysicalDataSourceConstraint
     * )};
     * Instead of returning the intersection of all available intervals, this method returns the union of them.
     *
     * @param ignoredConstraint  Constrains are ignored, because <tt>PermissiveAvailability</tt> returns as many
     * available intervals as possible by, for example, allowing missing intervals and returning unions of available
     * intervals
     *
     * @return the union of all available intervals
     */
    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint ignoredConstraint) {
        return getAllAvailableIntervals().values().stream()
                .map(SimplifiedIntervalList::new)
                .reduce(new SimplifiedIntervalList(), SimplifiedIntervalList::simplifyIntervals);
    }

    @Override
    public String toString() {
        return String.format("PermissiveAvailability with table name = %s", getName().asName());
    }
}
