// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import java.util.Set;

/**
 * External Table Config Template.
 */
public interface ExternalTableConfigTemplate {

    /**
     * Get physical table set.
     *
     * @return druid table info
     */
    Set<PhysicalTableInfoTemplate> getPhysicalTables();

    /**
     * Get logical table set.
     *
     * @return logical table info
     */
    Set<LogicalTableInfoTemplate> getLogicalTables();
}
