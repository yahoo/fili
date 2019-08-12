// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.config.luthier.names;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.util.EnumUtils;

/**
 * Used to instantiate a TableName object for Luthier Configuration.
 */
public class LuthierTableName implements TableName {
    private final String camelName;

    /**
     * Constructor.
     *
     * @param name the name of this Luthier Table
     */
    public LuthierTableName(String name) {
        camelName = EnumUtils.camelCase(name);
    }

    @Override
    public String asName() {
        return camelName;
    }
}
