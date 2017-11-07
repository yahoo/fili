// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

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
     * A human friendly name for this table. Meant to help users understand the name of this table, not used for
     * internal logic
     *
     * @return  A human friendly name for this table
     */
    String getLongName();

    /**
     * A description of this logical table. Meant to help users understand the purpose of this table, not used for
     * internal logic
     *
     * @return  A
     */
    String getDescription();
}
