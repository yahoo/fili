// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.table.ConstrainedTable;

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
     * @param tableName  The name of the table's single datasource
     */
    public TableDataSource(ConstrainedTable physicalTable, TableName tableName) {
        super(DefaultDataSourceType.TABLE, Collections.singleton(physicalTable));

        this.name = tableName.asName();
    }

    /**
     * Constructor.
     *
     * @param physicalTable  The physical table of the data source
     */
    public TableDataSource(ConstrainedTable physicalTable) {
        this(
                physicalTable,
                physicalTable.getDataSourceNames()
                        .stream()
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Non singleton DataSource table passed to TableDataSource constructor."
                                )
                        )
        );
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
