// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.names;

import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Hold the list of logical table names.
 */
public enum TestDimensionSearchLogicalTableName implements TableName {

    TABLE;

    private final String camelName;

    /**
     * Constructor.
     */
    TestDimensionSearchLogicalTableName() {
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
