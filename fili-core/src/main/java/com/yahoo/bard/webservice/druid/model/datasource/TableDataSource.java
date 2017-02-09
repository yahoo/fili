// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.table.ConcretePhysicalTable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Set;

/**
 * TableDataSource class.
 */
public class TableDataSource extends DataSource {

    private final String name;

    /**
     * Constructor.
     *
     * @param physicalTable  The physical table of the data source
     */
    public TableDataSource(ConcretePhysicalTable physicalTable) {
        super(DefaultDataSourceType.TABLE, Collections.singleton(physicalTable));

        this.name = physicalTable.getAvailability().getDataSourceNames().stream().findFirst().get().asName();
    }

    public String getName() {
        return name;
    }

    @Override
    @JsonIgnore
    public Set<String> getNames() {
        return super.getNames();
    }

    @Override
    @JsonIgnore
    public DruidQuery<?> getQuery() {
        return null;
    }
}
