// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Hold the list of logical table names.
 */
public enum DimensionSearchLogicalTableName implements TableName {

    TABLE;

    private final String camelName;

    /**
     * Constructor.
     */
    DimensionSearchLogicalTableName() {
        this.camelName = EnumUtils.camelCase(name());
    }

    /**
     * This logical table as a String.
     *
     * @return  The logical name as a String.
     */
    public String asName() {
        return camelName;
    }
}
