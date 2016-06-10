// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Set;

/**
 * TableDataSource class
 */
public class TableDataSource extends DataSource {

    private final String name;
    private final Set<String> names;

    public TableDataSource(String name) {
        super(DefaultDataSourceType.TABLE);

        this.name = name;
        this.names = Collections.singleton(name);
    }

    public String getName() {
        return name;
    }

    @Override
    @JsonIgnore
    public Set<String> getNames() {
        return names;
    }

    @Override
    @JsonIgnore
    public DruidQuery<?> getQuery() {
        return null;
    }
}
