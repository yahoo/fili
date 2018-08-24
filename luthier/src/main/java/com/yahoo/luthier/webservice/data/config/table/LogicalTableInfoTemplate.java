// Copyright 2018 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.luthier.webservice.data.config.table;

import com.yahoo.bard.webservice.data.time.Granularity;

import java.util.Set;

/**
 * Logical Table Info Template.
 */
public interface LogicalTableInfoTemplate {

    /**
     * Get physical table name.
     *
     * @return physical table name
     */
    String getName();

    /**
     * Get physical table description.
     *
     * @return physical table description
     */
    String getDescription();

    /**
     * Get apiMetrics this logical table has.
     *
     * @return a set of apiMetrics
     */
    Set<String> getApiMetrics();

    /**
     * Get physical tables this logical table depends on.
     *
     * @return a set of physical tables' name
     */
    Set<String> getPhysicalTables();

    /**
     * Get logical table's granularity.
     *
     * @return logical table's granularity
     */
    Set<Granularity> getGranularities();
}
