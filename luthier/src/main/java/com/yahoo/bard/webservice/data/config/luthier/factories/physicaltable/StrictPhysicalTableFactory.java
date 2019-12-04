// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.factories.physicaltable;

import com.yahoo.bard.webservice.application.luthier.LuthierConfigNode;
import com.yahoo.bard.webservice.data.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.table.ConfigPhysicalTable;
import com.yahoo.bard.webservice.table.StrictPhysicalTable;

/**
 * A factory that is used by default to support Simple (non-Composite) Physical Table.
 *
 * A strictPhysicalTable is available if and only if all of the columns
 * are available. This contrasts with the PermissivePhysicalTable where only
 * one column's availability has to be met.
 */
public class StrictPhysicalTableFactory extends SingleDataSourcePhysicalTableFactory {

    /**
     * Build a StrictPhysicalTable instance.
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
        return new StrictPhysicalTable(
                params.tableName,
                params.timeGrain,
                params.columns,
                params.logicalToPhysicalColumnNames,
                params.metadataService
        );
    }
}
