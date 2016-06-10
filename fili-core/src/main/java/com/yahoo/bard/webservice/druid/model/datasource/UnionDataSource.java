// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Represents a Druid Union data source.
 */
public class UnionDataSource extends DataSource {

    private final Set<String> names;

    public UnionDataSource(Set<String> names) {
        super(DefaultDataSourceType.UNION);

        this.names = Collections.unmodifiableSet(new LinkedHashSet<>(names));
    }

    @Override
    @JsonProperty(value = "dataSources")
    public Set<String> getNames() {
        return names;
    }

    @Override
    @JsonIgnore
    public DruidQuery<?> getQuery() {
        return null;
    }
}
