// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

import org.joda.time.ReadablePeriod;

import java.util.Optional;

/**
 * Interfaces to describe a logical table's fixed metadata for configuration purposes.
 */
public interface LogicalTableInfo {

    /**
     * A unique identifier for the table used in API path segments and internal dictionaries.
     *
     * @return  The name of the logical table.
     */
    String getName();

    /**
     * A human friendly name for this table.
     *
     * @return  A human friendly name for this table
     */
    String getLongName();

    /**
     * A description of this logical table.
     *
     * @return  A description of this table.
     */
    String getDescription();

    /**
     * A category grouping this table.
     *
     * @return  The name of a category for this table.
     */
    String getCategory();

    /**
     * The period of time into the past which data is expected to be retained.
     *
     * @return  A readable period describing the retention
     */
    Optional<ReadablePeriod> getRetention();
}
