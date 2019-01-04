// Copyright 2017 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.BadApiRequestException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to hold generator code for tables.
 */
public class DefaultTableBinder {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTableBinder.class);

    /**
     * Bind the table name against a Logical table in the table dictionary.
     *
     * @param tableName  Name of the logical table from the query
     * @param granularity  The granularity for this request
     * @param logicalTableDictionary  Dictionary to resolve logical tables against.
     *
     * @return  A logical table
     */
    public LogicalTable bindLogicalTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) {

        TableIdentifier tableId = new TableIdentifier(tableName, granularity);

        // Logical table must be in the logical table dictionary
        return logicalTableDictionary.get(tableId);
    }

    /**
     * Bind the table name against a Logical table in the table dictionary.
     *
     * @param tableName  Name of the logical table from the query
     * @param table  The bound logical table for this query
     * @param granularity  The granularity for this request
     * @param logicalTableDictionary  Dictionary to resolve logical tables against.
     *
     * @throws BadApiRequestException if invalid
     */
    public void validateLogicalTable(
            String tableName,
            LogicalTable table,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    ) throws BadApiRequestException {
        if (table == null) {
            LOG.debug(TABLE_UNDEFINED.logFormat(tableName));
            throw new BadApiRequestException(TABLE_UNDEFINED.format(tableName));
        }
    }
}
