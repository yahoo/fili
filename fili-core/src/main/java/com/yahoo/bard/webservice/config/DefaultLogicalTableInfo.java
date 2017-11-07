// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config;

/**
 * Default for LogicalTableInfo interface, everything just returns getName.
 */
@FunctionalInterface
public interface DefaultLogicalTableInfo extends LogicalTableInfo {

    /**
     * Provides a default behavior for getLongName, which is to return the unparsed table name.
     *
     * @return  A string representing the human readable name of this table.
     */
    default String getLongName() {
        return this.getName();
    }

    /**
     * Provides a default behavior for getDescription, which is to return the unparsed table name.
     *
     * @return  A string representing the description of this table's purpose.
     */
    default String getDescription() {
        return this.getName();
    }
}
