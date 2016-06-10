// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Set;

/**
 * DataSource base class
 */
public abstract class DataSource {
    private final DataSourceType type;

    public DataSource(DataSourceType type) {
        this.type = type;
    }

    public DataSourceType getType() {
        return type;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract Set<String> getNames();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract DruidQuery<?> getQuery();
}
