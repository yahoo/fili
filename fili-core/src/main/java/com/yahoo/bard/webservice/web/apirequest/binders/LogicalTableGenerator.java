// Copyright 2018 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.apirequest.binders;

import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_GRANULARITY_MISMATCH;
import static com.yahoo.bard.webservice.web.ErrorMessageFormat.TABLE_UNDEFINED;

import com.yahoo.bard.webservice.data.time.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.table.LogicalTableDictionary;
import com.yahoo.bard.webservice.table.TableIdentifier;
import com.yahoo.bard.webservice.web.BadApiRequestException;

/**
 * Generates a logical table given information to identify a logical table.
 */
public interface LogicalTableGenerator {

    /**
     * Extracts a specific logical table object given a valid table name and a valid granularity.
     *
     * @param tableName  logical table corresponding to the table name specified in the URL
     * @param granularity  logical table corresponding to the table name specified in the URL
     * @param logicalTableDictionary  Logical table dictionary contains the map of valid table names and table objects.
     *
     * @return Set of logical table objects.
     */
    LogicalTable generateTable(
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    );

    /**
     * Validates that the generated logical table matches the parameters that where used to generate it.
     *
     * @param table The table to validate
     * @param tableName Name of the logical table
     * @param granularity Granularity of the logical table
     * @param logicalTableDictionary Dictionary containing references to all constructed logical tables in the system.
     */
    void validateTable(
            LogicalTable table,
            String tableName,
            Granularity granularity,
            LogicalTableDictionary logicalTableDictionary
    );

    /**
     * Default implementations of this interface.
     */
    LogicalTableGenerator DEFAULT_LOGICAL_TABLE_GENERATOR = new LogicalTableGenerator() {
        @Override
        public LogicalTable generateTable(
                String tableName,
                Granularity granularity,
                LogicalTableDictionary logicalTableDictionary
        ) {
            LogicalTable generated = logicalTableDictionary.get(new TableIdentifier(tableName, granularity));

            // check if requested logical table grain pair exists
            if (generated == null) {
                String msg = TABLE_GRANULARITY_MISMATCH.logFormat(granularity, tableName);
                throw new BadApiRequestException(msg);
            }
            return generated;
        }

        @Override
        public void validateTable(
                LogicalTable table,
                String tableName,
                Granularity granularity,
                LogicalTableDictionary logicalTableDictionary
        ) {
            if (table == null) {
                throw new BadApiRequestException(TABLE_UNDEFINED.format(tableName));
            }
        }
    };
}
