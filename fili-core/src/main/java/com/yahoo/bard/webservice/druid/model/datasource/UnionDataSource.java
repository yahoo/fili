// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.table.ConstrainedTable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Represents a Druid Union data source.
 */
public class UnionDataSource extends DataSource {

    private static final Logger LOG = LoggerFactory.getLogger(UnionDataSource.class);

    /**
     * Constructor.
     *
     * @param physicalTable  The physical table of the data source
     */
    public UnionDataSource(ConstrainedTable physicalTable) {
        super(DefaultDataSourceType.UNION, physicalTable);
    }

    @Override
    @JsonProperty(value = "dataSources")
    public Set<String> getNames() {
        return super.getNames();
    }

    @Override
    @JsonIgnore
    public DruidQuery<?> getQuery() {
        return null;
    }
}
