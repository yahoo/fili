// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.table.ConstrainedTable;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/**
 * Represents a Druid Union data source.
 */
public class UnionDataSource extends DataSource {

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
}
