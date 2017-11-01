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
     * @return
     */
    String getName();

    /**
     * A human friendly name for this table inted
     * @return
     */
    String getLongName();

    String getDescription();
}
