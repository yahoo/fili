// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice;

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
     * @param tableName  The name of the table to look at.
     *
     * @return the name of the timestamp column.
     *
     * @throws SQLException if failed while reading database.
     */
    public static String getDateTimeColumn(Connection connection, String tableName) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + tableName + " LIMIT 1");
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData rsmd = resultSet.getMetaData();
        // todo as of now I've only tested this with timestamp
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            int jdbcType = rsmd.getColumnType(i);
            if (jdbcType == JDBCType.TIMESTAMP.getVendorTypeNumber()) {
                return rsmd.getColumnName(i);
            }
        }
        throw new IllegalStateException("No TIMESTAMP column was found");
    }

    /**
     * Converts a {@link ResultSet} to string.
     * From CalciteAssert
     * */
    public static class ResultSetFormatter {

        /**
         * Reads all the results in a {@link ResultSet} with name and value of columns.
         * NOTE: result set may not be able to be reset to beginning after rows have been read.
         *
         * @param resultSet  The resultSet to read from.
         *
         * @return a {@link StringBuilder} containing all the information read from the columns.
         *
         * @throws SQLException if failed while reading results.
         */
        public static StringBuilder format(ResultSet resultSet) throws SQLException {
            return format(resultSet, -1);
        }

        /**
         * Reads a number of the results in a {@link ResultSet} with name and value of columns.
         * NOTE: result set may not be able to be reset to beginning after rows have been read.
         *
         * @param resultSet  The resultSet to read from.
         * @param max  The max number of results to read from the resultSet.
         *
         * @return a {@link StringBuilder} containing all the information read from the columns.
         *
         * @throws SQLException if failed while reading results.
         */
        public static StringBuilder format(ResultSet resultSet, int max)
                throws SQLException {
            StringBuilder sb = new StringBuilder();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = 0;
            while (resultSet.next() && (count < max || max < 0)) {
                rowToString(sb, resultSet, metaData).append("\n");
                count++;
            }
            return sb;
        }

        /**
         * Converts one row to a string.
         * @param stringBuilder  The stringBuilder to write into.
         * @param resultSet  The resultSet to read from.
         * @param metaData  The metadata of the result set.
         *
         * @return the stringBuilder with results written into.
         *
         * @throws SQLException if results can't be read.
         */
        private static StringBuilder rowToString(
                StringBuilder stringBuilder,
                ResultSet resultSet,
                ResultSetMetaData metaData
        ) throws SQLException {
            int n = metaData.getColumnCount();
            if (n > 0) {
                for (int i = 1; ; i++) {
                    stringBuilder.append(metaData.getColumnLabel(i))
                            .append("=")
                            .append(resultSet.getString(i));
                    if (i == n) {
                        break;
                    }
                    stringBuilder.append("; ");
                }
            }
            return stringBuilder;
        }
    }
}
