// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.physicaltable;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.PermissivePhysicalTable;

/**
 * A factory that is used by default to support Simple (non-Composite) Physical Table.
 *
 * A permissivePhysicalTable is available as long as one column's availability
 * is met. This contrasts with the PermissivePhysicalTable where all of
 * the columns' availabilities need to be met.
 */
public class PermissivePhysicalTableFactory extends SingleDataSourcePhysicalTableFactory {

    /**
     * Build a PermissivePhysicalTable instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public ConfigPhysicalTable build(
            String name,
            LuthierConfigNode configTable,
            LuthierIndustrialPark resourceFactories
    ) {
        LuthierPhysicalTableParams params = buildParams(name, configTable, resourceFactories);
        return new PermissivePhysicalTable(
                params.tableName,
                params.timeGrain,
                params.columns,
                params.logicalToPhysicalColumnNames,
                params.metadataService
        );
    }
}
