// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.table.ConstrainedTable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Set;

/**
 * TableDataSource class.
 */
public class TableDataSource extends DataSource {

    /**
     * Constructor.
     *
     * @param physicalTable  The physical table of the data source
     */
    public TableDataSource(ConstrainedTable physicalTable) {
        super(DefaultDataSourceType.TABLE, physicalTable);
    }

    public String getName() {
        return getPhysicalTable().getTableName().asName();
    }

    @Override
    @JsonIgnore
    public Set<String> getNames() {
        return super.getNames();
    }
}
