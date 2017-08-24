// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.druid.model.query.DruidFactQuery;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Optional;
import java.util.Set;

/**
 * QueryDataSource.
 */
public class QueryDataSource extends DataSource {

    private final DruidFactQuery<?> query;

    /**
     * Constructor.
     *
     * @param query  Query that defines the DataSource.
     */
    public QueryDataSource(DruidFactQuery<?> query) {
        super(DefaultDataSourceType.QUERY, query.getDataSource().getPhysicalTable());

        this.query = query;
    }

    @Override
    @JsonIgnore
    public Set<String> getNames() {
        return query.getInnermostQuery().getDataSource().getNames();
    }

    @Override
    public Optional<? extends DruidFactQuery<?>> getQuery() {
        return Optional.ofNullable(query);
    }
}
