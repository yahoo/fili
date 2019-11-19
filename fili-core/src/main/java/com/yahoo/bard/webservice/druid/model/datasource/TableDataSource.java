// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOO_FEW_BACKING_DATA_SOURCES;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TOO_MANY_BACKING_DATA_SOURCES;

import com.yahoo.bard.webservice.table.ConstrainedTable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * TableDataSource class.
 */
public class TableDataSource extends DataSource {

    private static final Logger LOG = LoggerFactory.getLogger(TableDataSource.class);

    /**
     * Constructor.
     *
     * @param physicalTable  The physical table of the data source. It must have only 1 backing data source.
     */
    public TableDataSource(ConstrainedTable physicalTable) {
        super(DefaultDataSourceType.TABLE, physicalTable);
        if (physicalTable.getDataSourceNames().size() > 1) {
            LOG.error(TOO_MANY_BACKING_DATA_SOURCES.logFormat(getPhysicalTable()));
            throw new IllegalArgumentException(TOO_MANY_BACKING_DATA_SOURCES.format(getPhysicalTable()));
        }
    }

    public String getName() {
        return getPhysicalTable().getDataSourceNames().stream().findFirst()
                .orElseThrow(() -> {
                    LOG.error(TOO_FEW_BACKING_DATA_SOURCES.logFormat(getPhysicalTable()));
                    return new IllegalArgumentException(TOO_FEW_BACKING_DATA_SOURCES.format(getPhysicalTable()));
                }).asName();
    }

    @Override
    @JsonIgnore
    public Set<String> getNames() {
        return super.getNames();
    }
}
