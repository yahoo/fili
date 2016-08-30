// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.names;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Hold the list of logical table names.
 */
public enum WikiLogicalTableName implements TableName {

    WIKIPEDIA;

    private final String camelName;

    /**
     * Constructor.
     */
    WikiLogicalTableName() {
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
