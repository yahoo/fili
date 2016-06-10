// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.util.EnumUtils;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Legal data source types in Druid
 */
public enum DefaultDataSourceType implements DataSourceType {
    TABLE,
    QUERY,
    UNION
    ;

    final String jsonName;

    DefaultDataSourceType() {
        this.jsonName = EnumUtils.enumJsonName(this);
    }

    @JsonValue
    public String toJson() {
        return jsonName;
    }
}
