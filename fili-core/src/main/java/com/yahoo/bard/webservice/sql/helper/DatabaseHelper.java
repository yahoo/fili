// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Various functions to help with interacting with a database.
 */
public class DatabaseHelper {
    private static final int DATA_TYPE = 5;
    private static final int COLUMN_NAME = 4;

    /**
     * Private constructor - all methods static.
     */
    private DatabaseHelper() {
    }

    /**
     * Finds the name of the Timestamp column in a sql database.
     *
     * @param connection  The connection to the database.
     * @param schema  The name of the schema (i.e. "PUBLIC").
     * @param table  The name of the table (i.e. "table").
     *
     * @return the name of the timestamp column.
     *
     * @throws SQLException if failed while reading database.
     * @throws IllegalStateException if no {@link JDBCType#TIMESTAMP} column could be found.
     */
    public static String getTimestampColumn(Connection connection, String schema, String table)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        ResultSet columnInformation = metadata.getColumns(null, schema, table, null);
        while (columnInformation.next()) {
            if (columnInformation.getInt(DATA_TYPE) == JDBCType.TIMESTAMP.getVendorTypeNumber()) {
                return columnInformation.getString(COLUMN_NAME);
            }
        }
        throw new IllegalStateException("No TIMESTAMP column was found");
    }
}
