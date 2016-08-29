// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.names;

import com.yahoo.bard.webservice.data.config.names.TableName;

import java.util.Locale;

/**
 * Hold the list of raw Druid table names.
 */
public enum WikiDruidTableName implements TableName {
    WIKIPEDIA;

    private final String lowerCaseName;

    WikiDruidTableName() {
        this.lowerCaseName = name().toLowerCase(Locale.ENGLISH);
    }

    public String asName() {
        return lowerCaseName;
    }
}
