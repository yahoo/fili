// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.provider;

import com.yahoo.bard.webservice.data.time.TimeGrain;

import java.util.Set;

/**
 * A LogicalTableConfiguration defines configuration for a LogicalTable
 *
 * FIXME: should getTimeGrains() deal with time grains or granularities?
 */
public interface LogicalTableConfiguration {

    /**
     * Get the logical table name.
     *
     * @return the table name
     */
    String getName();

    /**
     * Get the time grains for this logical table.
     *
     * @return set of time grains
     */
    Set<TimeGrain> getTimeGrains();

    /**
     * Get the physical tables backing this logical table.
     *
     * @return set of physical table names
     */
    Set<String> getPhysicalTables();

    /**
     * Get the list of metrics this logical table provides.
     *
     * @return set of metric names
     */
    Set<String> getMetrics();
}
