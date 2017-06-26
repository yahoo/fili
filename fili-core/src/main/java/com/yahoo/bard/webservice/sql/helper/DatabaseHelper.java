// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.sql.helper;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Various functions to help with interacting with a database.
 */
public class DatabaseHelper {
    /**
     * Private constructor - all methods static.
     */
    private DatabaseHelper() {
    }

    /**
     * Finds the name of the Timestamp column in a sql database.
     *
     * @param connection  The connection to the database.
     * @param tableWithSchema  The name of the table with schema (i.e. "public"."table").
     *
     * @return the name of the timestamp column.
     *
     * @throws SQLException if failed while reading database.
     */
    public static String getTimestampColumn(Connection connection, String tableWithSchema)
            throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + tableWithSchema + " " +
                "LIMIT 1");
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            int jdbcType = resultSetMetaData.getColumnType(i);
            if (jdbcType == JDBCType.TIMESTAMP.getVendorTypeNumber()) {
                return resultSetMetaData.getColumnName(i);
            }
        }
        throw new IllegalStateException("No TIMESTAMP column was found");
    }
}
